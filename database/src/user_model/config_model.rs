



/*


----

$sym = [btc, eth, ltc];
$time = [1min, 5min, 15min];

$sym
    /? [open, high, low, close, volume]
    /! $time
;

----
Stage 1:

$sym [btc, eth, ltc]
    /? $_ [open, high, low, close, volume]
    /! $_ [1min, 5min, 15min]
;

----
Stage 2:

btc /?open !/1min
btc /?high !/1min
btc /?low !/1min
btc /?close !/1min

...

-----

$sym
    /? [high, low, close]
    /? $time
    /! &sym/&time/hlc3

----

Stage 1:

$sym [btc, eth, ltc]
    ?/ $time [1min, 5min, 15min]
    ?/ &sym/&time/hlc3





btc
    /? [close, volume]
    /? $time [1min, 5min, 15min]
    /! btc/&time/vwap

-----

 */

