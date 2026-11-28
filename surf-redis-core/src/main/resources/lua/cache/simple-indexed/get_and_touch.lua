-- KEYS
local idsKey     = KEYS[1]

-- ARGV
local ttl        = tonumber(ARGV[1])
local prefix     = ARGV[2]
local id         = ARGV[3]
local indexCount = tonumber(ARGV[4])

local valKey     = prefix .. ":__val__:" .. id
local raw        = redis.call('GET', valKey)
if not raw then
    return false
end

redis.call('SADD', idsKey, id)
redis.call('PEXPIRE', idsKey, ttl)
redis.call('PEXPIRE', valKey, ttl)

local i = 5
for _ = 1, indexCount do
    local idxName = ARGV[i]; i = i + 1
    local metaKey = prefix .. ":__meta__:" .. id .. ":" .. idxName
    redis.call('PEXPIRE', metaKey, ttl)
end

return raw
