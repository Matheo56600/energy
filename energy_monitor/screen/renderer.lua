local utils = require("utils")

local Renderer = {}

local ESC = "\27["
local COLORS = {
    reset = ESC .. "0m",
    green = ESC .. "32;1m",
    red = ESC .. "31;1m",
    yellow = ESC .. "33;1m",
    cyan = ESC .. "36;1m",
    gray = ESC .. "1;30m",
    white = ESC .. "37;1m",
}

local function field(snapshot, section, key)
    if type(snapshot) ~= "table" then
        return nil
    end
    local data = snapshot[section]
    if type(data) ~= "table" then
        return nil
    end
    return data[key]
end

local function energy(value, suffix)
    if value == nil then
        return "--"
    end
    return utils.formatEnergy(value, suffix)
end

local function line(text, color)
    return { text = text, color = color }
end

local function center(text, width)
    if #text >= width then
        return text
    end
    return string.rep(" ", (width - #text) // 2) .. text
end

local function statusInfo(state)
    if state.online then
        return "ONLINE", "green"
    end
    if state.snapshot then
        return "OFFLINE", "red"
    end
    if state.protocolError then
        return "PROTOCOLE INCOMPATIBLE", "red"
    end
    return "CONNEXION...", "yellow"
end

local function ageText(state, now)
    if not state.lastReceive then
        return "aucune donnee recue"
    end
    return string.format("il y a %.1f s", math.max(0, now - state.lastReceive))
end

local function generatorsText(snapshot)
    local active = field(snapshot, "generators", "active")
    local total = field(snapshot, "generators", "total")
    if active == nil or total == nil then
        return "--"
    end
    return string.format("%d / %d actifs", active, total)
end

local function buildSections(state, now, width)
    local snapshot = state.snapshot
    local statusText, statusColor = statusInfo(state)
    local invalid = field(snapshot, "generators", "invalid")

    local sections = {}

    local statusLines = { line("ETAT SYSTEME", "cyan"), line(statusText, statusColor) }
    if not state.online and state.snapshot then
        statusLines[#statusLines + 1] = line("CENTRAL OFFLINE", "red")
    end
    if snapshot and snapshot.bus == "down" then
        statusLines[#statusLines + 1] = line("Bus OC2 du central indisponible", "yellow")
    end
    sections[#sections + 1] = { name = "status", priority = 0, lines = statusLines }

    sections[#sections + 1] = {
        name = "production",
        priority = 0,
        lines = {
            line("PRODUCTION ACTUELLE", "cyan"),
            line(energy(field(snapshot, "production", "fe_t"), "/t"), "white"),
            line(energy(field(snapshot, "production", "fe_s"), "/s"), "white"),
        },
    }

    local generatorLines = { line("GENERATEURS", "cyan"), line(generatorsText(snapshot), "white") }
    if invalid and invalid > 0 then
        generatorLines[#generatorLines + 1] = line(string.format("%d sans reponse", invalid), "yellow")
    end
    sections[#sections + 1] = { name = "generators", priority = 0, lines = generatorLines }

    sections[#sections + 1] = {
        name = "storage",
        priority = 0,
        lines = {
            line("ENERGIE STOCKEE", "cyan"),
            line(energy(field(snapshot, "storage", "fe")), "white"),
        },
    }

    sections[#sections + 1] = {
        name = "cumulative",
        priority = 0,
        lines = {
            line("PRODUCTION CUMULEE", "cyan"),
            line(energy(field(snapshot, "cumulative", "fe")), "white"),
        },
    }

    sections[#sections + 1] = {
        name = "averages",
        priority = 2,
        lines = {
            line("MOYENNES", "cyan"),
            line("1 min : " .. energy(field(snapshot, "stats", "avg_1m_fe_s"), "/s"), "white"),
            line("1 h   : " .. energy(field(snapshot, "stats", "avg_1h_fe_s"), "/s"), "white"),
            line("Pic   : " .. energy(field(snapshot, "stats", "peak_fe_s"), "/s"), "white"),
        },
    }

    local sourceLines = { line("SOURCES", "cyan") }
    local sources = type(snapshot) == "table" and snapshot.sources or nil
    for _, entry in ipairs({ { "wind", "Eolien" }, { "solar", "Solaire" }, { "nuclear", "Nucleaire" } }) do
        local info = type(sources) == "table" and sources[entry[1]] or nil
        local text = "--"
        local color = "gray"
        if type(info) == "table" then
            if info.enabled then
                text = string.format("%d/%d  %s", info.active or 0, info.total or 0, energy(info.fe_s, "/s"))
                color = "white"
            else
                text = "desactive"
            end
        end
        sourceLines[#sourceLines + 1] = line(entry[2] .. " : " .. text, color)
    end
    sections[#sections + 1] = { name = "sources", priority = 1, lines = sourceLines }

    local updateColor = state.online and "white" or "yellow"
    sections[#sections + 1] = {
        name = "update",
        priority = 0,
        lines = {
            line("DERNIERE ACTUALISATION", "cyan"),
            line(ageText(state, now), updateColor),
        },
    }
    return sections
end

local function assemble(sections, withSpacing, width)
    local bar = string.rep("=", width)
    local lines = {
        line(bar, "gray"),
        line(center("MEKANISM POWER MONITOR", width), "white"),
        line(bar, "gray"),
    }
    for _, section in ipairs(sections) do
        if withSpacing then
            lines[#lines + 1] = line("", nil)
        end
        for _, entry in ipairs(section.lines) do
            lines[#lines + 1] = entry
        end
    end
    if withSpacing then
        lines[#lines + 1] = line("", nil)
    end
    lines[#lines + 1] = line(bar, "gray")
    return lines
end

local function buildCompact(state, now, rows)
    local snapshot = state.snapshot
    local statusText, statusColor = statusInfo(state)
    local feT = energy(field(snapshot, "production", "fe_t"), "/t")
    local feS = energy(field(snapshot, "production", "fe_s"), "/s")
    local lines = {
        line("ETAT  " .. statusText, statusColor),
        line("PROD  " .. feT .. "  " .. feS, "white"),
        line("GEN   " .. generatorsText(snapshot) .. "  STOCK " .. energy(field(snapshot, "storage", "fe")), "white"),
        line("CUMUL " .. energy(field(snapshot, "cumulative", "fe")), "white"),
        line("MAJ   " .. ageText(state, now), state.online and "white" or "yellow"),
    }
    if rows >= #lines + 1 then
        table.insert(lines, 1, line("MEKANISM POWER MONITOR", "white"))
    end
    while #lines > rows do
        table.remove(lines)
    end
    return lines
end

function Renderer.build(state, now, columns, rows, maxWidth)
    local width = math.max(20, math.min(columns, maxWidth or 72))
    local sections = buildSections(state, now, width)

    local candidates = {}
    for priority = 2, 0, -1 do
        local subset = {}
        for _, section in ipairs(sections) do
            if section.priority <= priority then
                subset[#subset + 1] = section
            end
        end
        candidates[#candidates + 1] = subset
    end

    for _, subset in ipairs(candidates) do
        for _, spacing in ipairs({ true, false }) do
            local lines = assemble(subset, spacing, width)
            if #lines <= rows then
                return lines, width
            end
        end
    end
    return buildCompact(state, now, rows), width
end

function Renderer.toString(lines, width, columns, useColor)
    local output = {}
    for _, entry in ipairs(lines) do
        local text = entry.text
        if #text > width then
            text = text:sub(1, width)
        end
        if useColor and entry.color then
            output[#output + 1] = COLORS[entry.color] .. text .. COLORS.reset .. ESC .. "K"
        else
            output[#output + 1] = text .. ESC .. "K"
        end
    end
    return table.concat(output, "\n")
end

return Renderer
