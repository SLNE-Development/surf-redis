local dataKey = KEYS[1]
local value   = ARGV[1]

if redis.call('LPOS', dataKey, value) then
    return 1
end

return 0
