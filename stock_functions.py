import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import datetime 

plt.style.use('ggplot')


def get_ohlc(ticker_name='GOOGL', start_date='2016-01-01', end_date=f"{datetime.datetime.now().date()}"):
    tickerData = yf.Ticker(ticker_name)
    df = tickerData.history(period='id', start=start_date, end=end_date).reset_index()
    return df 


def get_close_price(ticker_name='GOOGL', start_date='2016-01-01', end_date=f"{datetime.datetime.now().date()}"):
    tickerData = yf.Ticker(ticker_name)
    ser = tickerData.history(period='id', start=start_date, end=end_date)['Close']
    return pd.DataFrame(ser) 


def hey_ma(df,
           ma=20,
           col='Close',
           override_name=None,
           kind='simple',
           inplace=False,
          ):
    '''return a pandas.Series indicating the moving average for a given window
    
    INPUTS:
    df: DataFrame; a stock dataframe
    ma: integer; the interval with which to calculate the moving average
    col: string; the column from which to calculate the moving average
    override_name: None or string; whether to name the returned or plotted column (inputs label to plot call; name to Series.name)
    kind (either 's' or 'e'): whether to calculate the simple or exponential moving average
    '''

    ### Calculating SMA from `ma` argument
    if kind.casefold() in ['simple', 's']:
        output = df[col].rolling(ma).mean()
    ### Calculating EMA
    elif kind.casefold() in ['exponential', 'e', 'exp']:
        output = df[col].ewm(span=ma, adjust=False).mean()

    ### Interpreting `name` argument and setting it (if not passed)
    if not override_name:
        output.name=f'{kind.upper()[0]}MA{ma}'
    else:
        assert isinstance(override_name, str), 'name must be a string'
        output.name = override_name



    if inplace==False:
        return output
    else:
        df[output.name] = output


def add_bbs(df, 
           use_col='Close', 
           std_devs=2, 
           use_ma=20, 
           inplace=False
          ):
        
    '''df: the dataframe from which to calculate the moving average
    col: the column from which to calculate the moving average
    buffer: the number of standard deviations from which to calculate the bollinger bands
    MA: the number of days in the moving average
    add_dev: whether to add the SMA's standard deviation as a column to the dataframe
    plot: whether or not to call the SMA plot function'''
    
    #### DEFINE COLUMNS
   # the simple moving average returned from add_ma
    sma = hey_ma(df, col=use_col, ma=use_ma)
    
    # the standard deviation from the mean for the same window
    std_dev = df[use_col].rolling(window=use_ma).std() 
    
    upper = sma + (std_dev * std_devs)
    lower = sma - (std_dev * std_devs)
    upper.name = f'Upper BB{use_ma}'
    lower.name = f'Lower BB{use_ma}'
    # if add_std_col==True:
    #     df[f'{ma}STD'] = df[col].rolling(window=use_ma).std() 
    
    # if plot_bb==True:  
    #     plt.plot(upper, label = f'MA{ma}UPPER BB', lw=0.5)  
    #     plt.plot(lower, label = f'MA{ma}LOWER BB', lw=0.5)
    
    if inplace == True:
      df[upper.name], df[lower.name] = upper, lower
    else:   
        return pd.concat([upper, lower], axis=1)
    

def find_gold(df, 
              ax,
              col1='SMA20', 
              col2='SMA200',
              plot_points=True
             ):
    '''INPUTS:
        DFS: Dataframe OF OHLC DATA
        MA1, MA2: VECTORS FROM WHICH TO  up-/down-trends
        PLOT_UPS/DOWNS: WHETHER TO PLOT THE VALUES USING MATPLOTLIB
        RETURN_UPS/DOWNS: WHETHER TO RETURN DATETIME INDEX AND VALUE OF REVERSAL MOMENTS
        
    '''  
    def find_reversals():
        # two columns for comparison.
        ma1 = df[col1]
        ma2 = df[col2]
        
        # container for golden cross values for this stock; lists of date/price tuples.
        up_reversal = []
        down_reversal = []

        #### CROSS LOGIC     
        # instantiate an down_ind indicator; WHEN MA1<MA2. 
        down_ind = None
        up_ind = None
        # iterate through the timeseries 
        # at any given datetime index, if the 20MA is above the 200MA  
        for idx, row in df.iterrows():
            # if you're in a down trend
            if ma1.loc[idx] <= ma2.loc[idx]:
            # and the down indicator is not activate
                if down_ind == False:
                    # you're in a downtrend;
                    down_ind = True
                    # append the down-reversal
                    down_reversal.append((idx, ma1.loc[idx]))
        
            elif ma1.loc[idx] > ma2.loc[idx]:
            # you're in an uptrend
                if down_ind == True: # previously a downtrend
                    up_reversal.append((idx, ma1.loc[idx])) # note the upwards reversal
                down_ind = False # in either case, you're in an uptrend now
        return up_reversal, down_reversal

    ### PLOTTING

    def plot_cross_points(up_reversal_points, down_reversal_points, ax=ax):
        # if up_reversal_points==True:
            # df[f'MA{ma1}'] OR {ma2} should be equal to the up_reversal
        # [ax.axhline(x[1], lw=0.35, ls='--', c='green') for x in up_reversal_points]
            #[plt.axvline(x[0], lw=0.75, ls='--', c='gold', label=round(x[1], 2)) for x in up_reversal]
        [ax.scatter(x[0], x[1], c='green', marker='x') for x in up_reversal_points] #, label=round(x[1], 2)) for x in up_reversal]
       
        # if down_reversal_points==True:
        # [ax.axhline(x[1], lw=0.35, ls='--', c='red') for x in down_reversal_points]   # for x in down_reversal]
            #[plt.axvline(x[0], lw=0.75, ls='--', c='black', label=round(x[1], 2)) for x in blacks]
        [ax.scatter(x[0], x[1], c='red', marker='x') for x in down_reversal_points]   # label=round(x[1], 2)) for x in down_reversal]   
        ### RETURNING LIST OF TUPLE PAIRS    
        
        # for i in up_reversal_points + down_reversal_points:
        #     ax.annotate(round(i[1], 2), (i[0], i[1]))

    up_reversals, down_reversals = find_reversals()

    if plot_points == True:
        plot_cross_points(up_reversals, down_reversals)

    return up_reversals, down_reversals

def plot_mean_reversals(ticker_symbol,
                start_date='2018-01-01',
                end_date=f"{datetime.datetime.now().date()}",
                short_ma=20,
                long_ma=200,
                show_bbs=False):
    
    # extract
    df = get_close_price(ticker_symbol, start_date, end_date)

    # transform
    hey_ma(df, short_ma, kind='simple', inplace=True)
    hey_ma(df, long_ma, kind='simple', inplace=True)

    if show_bbs == True:
        add_bbs(df, use_ma=short_ma, inplace=True)

    # load figure
    fig, ax = plt.subplots()
    ax.set_box_aspect(.4)

    colors = ['#1f77b4', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf','#ff7f0e', '#d62728', '#2ca02c', ]
    for x, color in zip(df, colors):
        ax.plot(df[x], label=x, lw=0.5, color=color)
    # ax.plot(df, lw=0.5)
    ax.set_title(f'SMA{short_ma}/SMA{long_ma} Mean Reversal for {ticker_symbol.upper()}')
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')
    up_reversals, down_reversals = find_gold(df, ax, col1=f'SMA{short_ma}', col2=f'SMA{long_ma}')
    ax.legend()

    return fig, ax, up_reversals, down_reversals

