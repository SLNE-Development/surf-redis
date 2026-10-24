-- KEYS
local setKey  = KEYS[1]

-- ARGV
local ttl     = tonumber(ARGV[1])

local members = redis.call('SMEMBERS', setKey)
redis.call('PEXPIRE', setKey, ttl)

return members
