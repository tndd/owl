# Processor
Processing raw data for analysis.
## price_fluctuation
Record summarizing the rate of change for n days.
### Table
name | type | describe
-- | -- | --
o0 | R | Percentage change open price "now" basis point from the previous day.
o1 | R | Percentage change open price in basis points between "1" day before and the day before.
... | ... | ...
o{n} | R | Percentage change open price in basis points between "n" day before and the day before.
h0 | R | Percentage change high price "now" basis point from the previous day.
h1 | R | Percentage change high price in basis points between "1" day before and the day before.
... | ... | ...
h{n} | R | Percentage change high price in basis points between "n" day before and the day before.
l0 | R | Percentage change low price "now" basis point from the previous day.
l1 | R | Percentage change low price in basis points between "1" day before and the day before.
... | ... | ...
l{n} | R | Percentage change low price in basis points between "n" day before and the day before.
c0 | R | Percentage change close price "now" basis point from the previous day.
c1 | R | Percentage change close price in basis points between "1" day before and the day before.
... | ... | ...
c{n} | R | Percentage change close price in basis points between "n" day before and the day before.
v0 | R | Percentage change volume "now" basis point from the previous day.
v1 | R | Percentage change volume between "1" day before and the day before.
... | ... | ...
v{n} | R | Percentage volume between "n" day before and the day before.
avg_s | R | Average of closing prices over a {short} period of time.
avg_m | R | Average of closing prices over a {middle} period of time.
avg_l | R | Average of closing prices over a {long} period of time.
avg_v | R | The average of volume over a {long} period of time.

### Defalult variable value
variable | value | description
-- | -- | --
n | 10 | It goes back to "n" days ago.
short | 6 | "short"-term range in average calculation.
middle | 36 | "middle"-term range in average calculation.
long | 216 | "lomg"-term range in average calculation.
