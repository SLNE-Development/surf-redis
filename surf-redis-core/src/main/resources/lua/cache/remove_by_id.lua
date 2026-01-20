-- ARGV:
-- 1: prefix
-- 2: id
-- 3: indexCount
-- 4..: indexName1..indexNameN
--
-- Returns:
--   [1] removed (0/1)
--   [2..] touched index entries as "indexName<0>indexValue"

local prefix = ARGV[1]
local id = ARGV[2]
local indexCount = tonumber(ARGV[3])
local sep = string.char(0)

local idsKey = prefix .. ":__ids__"
local valKey = prefix .. ":__val__:" .. id

local removedVal = redis.call('UNLINK', valKey)
local removedId = redis.call('SREM', idsKey, id)

local touched = {}
local i = 4

for _ = 1, indexCount do
  local idxName = ARGV[i]; i = i + 1
  local metaKey = prefix .. ":__meta__:" .. id .. ":" .. idxName
  
  local vals = redis.call('SMEMBERS', metaKey)
  for _, v in ipairs(vals) do
    local idxKey = prefix .. ":__idx__:" .. idxName .. ":" .. v
    redis.call('SREM', idxKey, id)
    if redis.call('SCARD', idxKey) == 0 then
      redis.call('DEL', idxKey)
    end
    touched[#touched + 1] = idxName .. sep .. v
  end
  
  redis.call('DEL', metaKey)
end

if redis.call('SCARD', idsKey) == 0 then
  redis.call('DEL', idsKey)
end

local removed = 0
if removedVal > 0 or removedId > 0 then
  removed = 1
end

local res = { removed }
for _, t in ipairs(touched) do
  res[#res + 1] = t
end
return res