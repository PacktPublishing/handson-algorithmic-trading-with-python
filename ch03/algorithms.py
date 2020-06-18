import quantopian.algorithm as algo

from quantopian.pipeline import Pipeline

from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import Q500US

from quantopian.pipeline.data import morningstar
from quantopian.pipeline.classifiers.morningstar import Sector

from quantopian.pipeline.factors import BollingerBands

def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    
    # Setting initial trading parameters at start up.
    # Number of symbols to trade.
    context.NUM_SYMBOLS = 10
    
    # Size of each trade in $s.
    context.TRADE_SIZE = 100000

    # Minimum profit to ask on a position for in percentage.
    context.MIN_PROFIT_PERCENT = 1
    
    # Maximum loss to allow on a position in percentage.
    context.MAX_LOSS_PERCENT = 1
    
    # Counters to track winners and losers in number of shares units.
    context.winners = 0
    context.losers = 0
    
    # Set custom commission.
    set_commission(commission.PerShare(cost=0.00025))
    
    # Build Bollinger bands pipeline for top NUM_SYMBOLS stocks by market cap in tech sector.
    pipe_bbands = make_pipeline(context)
    algo.attach_pipeline(pipe_bbands, 'pipe_bbands')

    # Schedule the generate_entries method to be called every day 30 minutes after market open.
    schedule_function(generate_entries, date_rules.every_day(), time_rules.market_open(minutes=30))


def make_pipeline(context):
    """
    Builds a Pipeline with Bollinger bands for top NUM_SYMBOLS stocks by market cap in the tech sector.
    Starts out from the Q500US base universe.
    """
    
    # Base universe of top 500 US stocks.
    base_universe_filter = Q500US()

    # Stocks of only tech sector.
    tech_sector = Sector(mask=base_universe_filter)
    tech_universe_filter = base_universe_filter & tech_sector.eq(311)

    # Top 10 tech stocks with largest market cap.
    mkt_cap_filter = morningstar.valuation.market_cap.latest
    top_mkt_cap_tech_filter = mkt_cap_filter.top(context.NUM_SYMBOLS, mask=tech_universe_filter)

    # Bollinger band factor with Stdev factor 2.
    lower_band_factor, middle_factor, upper_band_factor = BollingerBands(window_length=22, k=2, mask=top_mkt_cap_tech_filter)

    # Percent difference between (price, lower_band) and (price, upper_band).
    price = USEquityPricing.close.latest
    buy_percent_factor = ((lower_band_factor - price)*100)/price
    sell_percent_factor = ((price - upper_band_factor)*100)/price

    # Mean reversion buy and sell filters.
    # Sell when price exceeds upper-band and buy when price is below lower-band.
    buy_filter = buy_percent_factor > 0
    sell_filter = sell_percent_factor > 0

    # Build and return the Pipeline.
    pipe_bbands = Pipeline(columns={'buy_percent': buy_percent_factor,
                                    'lower_band': lower_band_factor,
                                    'buy': buy_filter,
                                    'price': price,
                                    'sell': sell_filter,
                                    'upper_band': upper_band_factor,
                                    'sell_percent': sell_percent_factor}, screen=top_mkt_cap_tech_filter)
    
    return pipe_bbands


def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    pipe_bbands = algo.pipeline_output('pipe_bbands')    

    # Find list of symbols to buy/sell.
    context.buy = pipe_bbands[pipe_bbands['buy']].index.tolist()
    context.sell = pipe_bbands[pipe_bbands['sell']].index.tolist()


def check_profit_loss(context, data):
    """
    Check open positions to see if they need to be closed because of enough profit or bailed due to too much loss.
    """
    
    for security in context.portfolio.positions:
        position = context.portfolio.positions[security]
        current_price = data.current(security, 'price')
        
        # Calculate unrealized pnl on this position based on current market price in percentage units.
        price_diff_percent = ((current_price - position.cost_basis)*100)/current_price

        # Calculate if this position has enough profit to be closed.
        profit = ((position.amount > 0 and price_diff_percent > context.MIN_PROFIT_PERCENT) or
                  (position.amount < 0 and -price_diff_percent > context.MIN_PROFIT_PERCENT))

        # Calculate if this position has enough loss to be closed.
        loss = ((position.amount > 0 and -price_diff_percent > context.MAX_LOSS_PERCENT) or
                (position.amount < 0 and price_diff_percent > context.MAX_LOSS_PERCENT))

        if security not in context.buy and security not in context.sell and (profit or loss):
            log.info('Flattening {} position.amount {} position.price {} current.price {} profit {} loss {}'.format(security, position.amount, position.cost_basis, current_price, profit, loss))
            
            # Update winners and losers counters.
            if profit:
                context.winners += abs(position.amount * context.MIN_PROFIT_PERCENT)
            else:
                context.losers += abs(position.amount * context.MAX_LOSS_PERCENT)
                
            # Flatten position for this security.
            order_target_percent(security, 0)

            # Plot updated winners and losers variables.
            record_vars(context, data)
    
    
def generate_entries(context, data):
    """
    Execute orders to enter positions according to our schedule_function() timing.
    """
    pipe_bbands = algo.pipeline_output('pipe_bbands')
    buy_signals = pipe_bbands['buy']
    sell_signals = pipe_bbands['sell']
    if buy_signals.any():
        log.info('\n{}'.format(pipe_bbands[buy_signals][['buy', 'buy_percent', 'lower_band', 'price']]))
    if sell_signals.any():
        log.info( '\n{}'.format(pipe_bbands[sell_signals][['sell', 'sell_percent', 'upper_band', 'price']]))
        
    for security in context.buy:
        if security not in context.portfolio.positions:
            # Buy TRADE_SIZE amount of shares.
            order_target_value(security, context.TRADE_SIZE)
            log.info('Buying {}'.format(security))
        
    for security in context.sell:
        if security not in context.portfolio.positions:
            # Sell TRADE_SIZE amount of shares.
            order_target_value(security, -context.TRADE_SIZE)
            log.info('Selling {}'.format(security))

def record_vars(context, data):
    """
    Plot variables.
    """

    # Plot the winners, losers and win-lose ratio
    record(Winners=context.winners)
    record(Losers=context.losers)
    if context.losers:
        record(WinLoseRatio=context.winners/context.losers)


def handle_data(context, data):
    """
    Called every minute.
    """
    
    # Check if any of our positions need to be exited because they are either profitable enough or have lost too much money.
    # We check on our positions every minute as market data updates flow in.
    check_profit_loss(context, data)
