# specs

build a CLOB
handle order types: limit, market, cancel

open on even minutes, 12:00:00, send welcome message
close on odd minutes, 12:01:00, send goodbye message

# rules

orders received on off-hours are rejected
limit queue - time priority
market orders - fill all available

# messages

send order confirmation to participants
published executed trades on ticker tape
send fill/executed message to participants

# services

save orderbook snapshot every 100 events
save orderbook delta on every event
log all messages

# limit order

# market order

# cancel order

# data pipe

one socket to send exchange updates
one socket to send private trade confirmation and fill & receive orders
one socket to publish trade execution
