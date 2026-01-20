-- ARGV:
-- 1: prefix
-- 2: id
-- 3: json
-- 4: ttlMillis
-- 5: indexCount
-- Then for each index:
--   name, valueCount, value1..valueN
--
-- Returns:
--   [1] wasNew (0/1)  (based on SADD to ids-set)
--   [2..] touched index entries as "indexName<0>indexValue"

local prefix = ARGV[1]
local id = ARGV[2]
local json = ARGV[3]
local ttl = tonumber(ARGV[4])
local indexCount = tonumber(ARGV[5])
local sep = string.char(0)

local idsKey = prefix .. ":__ids__"
local valKey = prefix .. ":__val__:" .. id

-- Upsert value with TTL
redis.call('SET', valKey, json, 'PX', ttl)

-- Canonical presence tracking
local wasNew = redis.call('SADD', idsKey, id)
redis.call('PEXPIRE', idsKey, ttl)

local touched = {}
local i = 6

for _ = 1, indexCount do
  local idxName = ARGV[i]; i = i + 1
  local newCount = tonumber(ARGV[i]); i = i + 1
  
  local metaKey = prefix .. ":__meta__:" .. id .. ":" .. idxName
  
  local oldVals = redis.call('SMEMBERS', metaKey)
  local oldMap = {}
  for _, v in ipairs(oldVals) do
    oldMap[v] = true
  end
  
  local newMap = {}
  local newVals = {}
  for _ = 1, newCount do
    local v = ARGV[i]; i = i + 1
    if not newMap[v] then
      newMap[v] = true
      newVals[#newVals + 1] = v
    end
  end
  
  -- Remove from index-sets that are no longer valid
  for _, v in ipairs(oldVals) do
    if not newMap[v] then
      local idxKey = prefix .. ":__idx__:" .. idxName .. ":" .. v
      redis.call('SREM', idxKey, id)
      if redis.call('SCARD', idxKey) == 0 then
        redis.call('UNLINK', idxKey)
      else
        redis.call('PEXPIRE', idxKey, ttl)
      end
      touched[#touched + 1] = idxName .. sep .. v
    end
  end
  
  -- Add/refresh current index-sets
  for _, v in ipairs(newVals) do
    local idxKey = prefix .. ":__idx__:" .. idxName .. ":" .. v
    
    local added = redis.call('SADD', idxKey, id)
    if added == 1 then
      touched[#touched + 1] = idxName .. sep .. v
    end
    redis.call('PEXPIRE', idxKey, ttl)
  end
  
  -- Rewrite meta-set
  redis.call('DEL', metaKey)
  if #newVals > 0 then
    redis.call('SADD', metaKey, unpack(newVals))
  end
  redis.call('PEXPIRE', metaKey, ttl)
end

local res = { wasNew }
for _, t in ipairs(touched) do
  res[#res + 1] = t
end
return res