local scriptDir = (arg and arg[0] or ""):match("^(.*)/[^/]*$") or "."
package.path = scriptDir .. "/?.lua;" .. package.path

local utils = require("utils")
local Renderer = require("renderer")
local protocol = require("protocol")

utils.setConversion(2, 5)

local passed, failed = 0, 0

local function check(name, condition, detail)
    if condition then
        passed = passed + 1
        print("[OK]    " .. name)
    else
        failed = failed + 1
        print("[ECHEC] " .. name .. (detail and (" -> " .. tostring(detail)) or ""))
    end
end

local function snapshot(feSecond)
    return {
        type = "snapshot",
        status = "online",
        bus = "ok",
        production = { j_t = feSecond / 8, fe_t = feSecond / 20, fe_s = feSecond },
        generators = { total = 30, active = 24, invalid = 0, stale = 0 },
        storage = { fe = 1250000, capacity_fe = 5000000 },
        cumulative = { fe = 842730000000 },
        stats = { avg_1m_fe_s = feSecond, avg_1h_fe_s = feSecond, peak_fe_s = feSecond },
        sources = { wind = { enabled = true, total = 30, active = 24, fe_t = 1, fe_s = 20 },
            solar = { enabled = false }, nuclear = { enabled = false } },
    }
end

local function joined(lines)
    local parts = {}
    for _, entry in ipairs(lines) do
        parts[#parts + 1] = entry.text
    end
    return table.concat(parts, "\n")
end

local online = { snapshot = snapshot(249000), lastReceive = 100, online = true }
local lines, width = Renderer.build(online, 100.4, 80, 30, 72)
local text = joined(lines)
check("Ecran: titre", text:find("MEKANISM POWER MONITOR", 1, true) ~= nil)
check("Ecran: ONLINE", text:find("ONLINE", 1, true) ~= nil and text:find("OFFLINE", 1, true) == nil)
check("Ecran: production kFE", text:find("249.00 kFE/s", 1, true) ~= nil, text)
check("Ecran: generateurs", text:find("24 / 30 actifs", 1, true) ~= nil)
check("Ecran: cumul GFE", text:find("842.73 GFE", 1, true) ~= nil)
check("Ecran: age", text:find("il y a 0.4 s", 1, true) ~= nil)
check("Ecran: solaire desactive", text:find("Solaire : desactive", 1, true) ~= nil)
check("Ecran: largeur respectee", width <= 72)

local offline = { snapshot = snapshot(249000), lastReceive = 100, online = false }
lines = Renderer.build(offline, 130, 80, 30, 72)
text = joined(lines)
check("Ecran: CENTRAL OFFLINE", text:find("CENTRAL OFFLINE", 1, true) ~= nil)

local waiting = { snapshot = nil, lastReceive = nil, online = false }
lines = Renderer.build(waiting, 1, 80, 30, 72)
text = joined(lines)
check("Ecran: attente sans donnees", text:find("CONNEXION...", 1, true) ~= nil and text:find("--", 1, true) ~= nil)

for _, size in ipairs({ { 24, 80 }, { 18, 60 }, { 12, 40 }, { 6, 30 }, { 4, 20 } }) do
    local rows, columns = size[1], size[2]
    local built, builtWidth = Renderer.build(online, 100.4, columns, rows, 72)
    local rendered = Renderer.toString(built, builtWidth, columns, true)
    local count = select(2, rendered:gsub("\n", "\n")) + 1
    check(string.format("Ecran %dx%d: tient dans la taille (%d lignes)", columns, rows, count),
        count <= rows and builtWidth <= columns, count)
end

for _, value in ipairs({ 950, 15400, 2.8e6, 4.2e9, 1.1e12 }) do
    lines = Renderer.build({ snapshot = snapshot(value), lastReceive = 1, online = true }, 1, 80, 30, 72)
    check("Ecran unite pour " .. value, joined(lines):find("FE/s", 1, true) ~= nil)
end

print(string.format("\n%d reussis, %d echecs", passed, failed))
os.exit(failed == 0 and 0 or 1)
