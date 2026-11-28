-- KEYS
local idsKey = KEYS[1]

-- ARGV
local ttl    = tonumber(ARGV[1])
local id     = ARGV[2]
local prefix = ARGV[3]

local valKey = prefix .. ":__val__:" .. id

local raw    = redis.call('GET', valKey)
if not raw then
    return false
end

redis.call('SADD', idsKey, id)
redis.call('PEXPIRE', idsKey, ttl)
redis.call('PEXPIRE', valKey, ttl)

return raw
