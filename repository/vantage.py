import os
from dotenv import load_dotenv
from alpha_vantage.timeseries import TimeSeries
import matplotlib.pyplot as plt

load_dotenv()

ts = TimeSeries(key=os.getenv('ALPVTG_API_KEY'), output_format='pandas')
data, meta_data = ts.get_intraday(symbol='MSFT',interval='1min', outputsize='full')
data['4. close'].plot()
plt.title('Intraday Times Series for the MSFT stock (1 min)')
plt.tight_layout()
plt.grid()
plt.show()
