local split = function (s, delimiter)
    local result = {}
    for match in (s..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match)
    end
    return result
end

local concat = function (t1, t2)
    if type(t2) ~= "table" then
        table.insert(t1, t2)
    else
        for k,v in ipairs(t2) do
            table.insert(t1, v)
        end
    end
    return t1
end

local chunk_size = 7000

local execute_redis_command_in_chunks = function (command, args)
    local results = {}
    local chunk_result = {}
    if #command == 1 then
        for i = 1, #args, chunk_size do
            chunk_result = redis.call(command[1],
                                      unpack(args, i, math.min(i + chunk_size - 1, #args)))
            results = concat(results, chunk_result)
        end
    elseif #command == 2 then
        for i = 1, #args, chunk_size do
            chunk_result = redis.call(command[1], command[2],
                                      unpack(args, i, math.min(i + chunk_size - 1, #args)))
            results = concat(results, chunk_result)
        end
    elseif #command == 3 then
        for i = 1, #args, chunk_size do
            chunk_result = redis.call(command[1], command[2], command[3],
                                      unpack(args, i, math.min(i + chunk_size - 1, #args)))
            results = concat(results, chunk_result)
        end
    end
    return results
end

redis.call("DEL", "tmp", "other_tmp")
for i, column in ipairs(KEYS) do
    local labels = split(ARGV[i], ",")
    if i == 1 then
        -- Initialize temporary set with group ids
        local group_ids
        if column == "GroupId" then
            group_ids = labels
            execute_redis_command_in_chunks({"SADD", "tmp"}, group_ids)
        else
            -- Recover group ids associated with the secondary index
            local set_keys = {}
            for i, label in ipairs(labels) do
                set_keys[i] = column .. ":" .. label
            end
            execute_redis_command_in_chunks({"SUNIONSTORE", "tmp", "tmp"}, set_keys)
        end
    else
        -- Recover group ids associated with the secondary index
        local set_keys = {}
        for i, label in ipairs(labels) do
            set_keys[i] = column .. ":" .. label
        end
        execute_redis_command_in_chunks({"SUNIONSTORE", "other_tmp", "other_tmp"}, set_keys)
        -- Keep only those group ids belonging to the intersection
        redis.call("SINTERSTORE", "tmp", "tmp", "other_tmp")
    end
end

local intersection = redis.call("SMEMBERS", "tmp")
local enc_tuples = execute_redis_command_in_chunks({"HMGET", "wrapped_with_mapping"},
                                                   intersection)
return enc_tuples
