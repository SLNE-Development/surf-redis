-- ARGV:
-- 1: prefix
-- 2: targetIndexName
-- 3: targetIndexValue
-- 4: indexCount
-- 5..: indexName1..indexNameN
--
-- Returns:
--   [1] removedCount (integer)
--   [2..] removedIds (list of IDs that were removed)

local prefix = ARGV[1]
local targetName = ARGV[2]
local targetValue = ARGV[3]
local indexCount = tonumber(ARGV[4])

local idsKey = prefix .. ":__ids__"
local targetKey = prefix .. ":__idx__:" .. targetName .. ":" .. targetValue

local removedIds = {}
local removedCount = 0

while true do
  local id = redis.call('SPOP', targetKey)
  if not id then break end
  
  redis.call('DEL', prefix .. ":__val__:" .. id)
  redis.call('SREM', idsKey, id)
  
  local j = 5
  for _ = 1, indexCount do
    local idxName = ARGV[j]; j = j + 1
    local metaKey = prefix .. ":__meta__:" .. id .. ":" .. idxName
    local vals = redis.call('SMEMBERS', metaKey)
    
    for _, v in ipairs(vals) do
      local idxKey = prefix .. ":__idx__:" .. idxName .. ":" .. v
      redis.call('SREM', idxKey, id)
       if redis.call('SCARD', idxKey) == 0 then
         redis.call('DEL', idxKey)
       end
    end
    
    redis.call('DEL', metaKey)
  end
  
  removedCount = removedCount + 1
  removedIds[#removedIds + 1] = id
end

if redis.call('SCARD', idsKey) == 0 then
  redis.call('DEL', idsKey)
end

local res = { removedCount }
for _, id in ipairs(removedIds) do
  res[#res + 1] = id
end
return res