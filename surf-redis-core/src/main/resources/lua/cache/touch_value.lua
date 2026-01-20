-- ARGV:
-- 1: prefix
-- 2: id
-- 3: ttlMillis
-- 4: indexCount
-- 5..: indexName1..indexNameN
--
-- Returns:
--   1 if value exists, else 0

local prefix = ARGV[1]
local id = ARGV[2]
local ttl = tonumber(ARGV[3])
local indexCount = tonumber(ARGV[4])

local valKey = prefix .. ":__val__:" .. id
local idsKey = prefix .. ":__ids__"

if redis.call('EXISTS', valKey) == 0 then
  return 0
end

-- Keep canonical ids tracking alive (and repair if missing)
redis.call('SADD', idsKey, id)
redis.call('PEXPIRE', idsKey, ttl)

-- Touch value
redis.call('PEXPIRE', valKey, ttl)

-- Touch meta keys
local i = 5
for _ = 1, indexCount do
  local idxName = ARGV[i]; i = i + 1
  local metaKey = prefix .. ":__meta__:" .. id .. ":" .. idxName
  redis.call('PEXPIRE', metaKey, ttl)
end

return 1