local socket = require("socket")

local FakeBus = {}
FakeBus.__index = FakeBus

function FakeBus.new(count, jPerTick, latency)
    local self = setmetatable({
        generators = {},
        nextIndex = 1,
        jPerTick = jPerTick,
        latency = latency or 0,
        calls = 0,
    }, FakeBus)
    for _ = 1, count do
        self:add()
    end
    return self
end

function FakeBus:add()
    local index = self.nextIndex
    self.nextIndex = index + 1
    self.generators[index] = {
        id = string.format("fake-%04d", index),
        active = true,
        jPerTick = self.jPerTick,
        x = index,
        y = 64,
        z = 0,
    }
    return index
end

function FakeBus:remove(index)
    self.generators[index] = nil
end

function FakeBus:setActive(index, active)
    self.generators[index].active = active
end

function FakeBus:setProduction(jPerTick)
    self.jPerTick = jPerTick
    for _, generator in pairs(self.generators) do
        generator.jPerTick = jPerTick
    end
end

function FakeBus:list()
    local result = {}
    for _, generator in pairs(self.generators) do
        result[#result + 1] = {
            deviceId = generator.id,
            typeNames = { "mekanism_wind_generator", "mekanism_generator" },
        }
    end
    table.sort(result, function(a, b)
        return a.deviceId < b.deviceId
    end)
    return result
end

function FakeBus:invoke(deviceId, methodName)
    self.calls = self.calls + 1
    if self.latency > 0 then
        socket.sleep(self.latency)
    end
    if methodName ~= "getGeneratorData" then
        error("unknown method " .. tostring(methodName), 0)
    end
    for _, generator in pairs(self.generators) do
        if generator.id == deviceId then
            local production = generator.active and generator.jPerTick or 0
            return {
                valid = true,
                type = "wind_generator",
                active = generator.active,
                production_fe_t = math.floor(production * 2 / 5 + 0.5),
                production_fe_s = math.floor(production * 2 / 5 + 0.5) * 20,
                production_j_t = production,
                max_output_fe_t = 0,
                max_output_j_t = 0,
                stored_fe = 0,
                stored_j = production,
                capacity_fe = 0,
                capacity_j = production * 100,
                x = generator.x,
                y = generator.y,
                z = generator.z,
                dimension = "minecraft:overworld",
            }
        end
    end
    error("no device with id " .. tostring(deviceId), 0)
end

function FakeBus:close()
end

return FakeBus
