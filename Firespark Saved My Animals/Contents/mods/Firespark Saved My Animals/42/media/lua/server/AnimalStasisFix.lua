-- =============================================================================
-- Animal Stasis Fix (Build 42)
-- 
-- Eliminates offline animal catch-up starvation, thirst drain, health decay,
-- and hutch filth spikes using engine-native streaming detection, a 5-second
-- post-load buffer, and a staggered 1-animal-per-tick FIFO queue.
-- 
-- Compatible with Singleplayer and Dedicated Servers.
-- =============================================================================

if isClient() and not isServer() then return end

AnimalStasisFix = AnimalStasisFix or {}
AnimalStasisFix.VERSION = "1.0.1"

-- Configuration
AnimalStasisFix.SCAN_INTERVAL_MS = 1000      -- Periodic streaming check (1 second)
AnimalStasisFix.BUFFER_DURATION_MS = 5000    -- 5-second buffer after zone is re-streamed
AnimalStasisFix.DEBUG = true                 -- Log key state transitions to console.txt

-- State tracking
AnimalStasisFix.zoneState = {}               -- [zoneIdStr] = { isStreamed = bool, state = string, bufferEndTime = number }
AnimalStasisFix.queue = {}                   -- FIFO queue of IsoAnimal objects to restore mortality
AnimalStasisFix.queuedSet = {}               -- [animalId] = true (prevents duplicate enqueuing)
AnimalStasisFix.lastScanMs = 0
local lastChunkCheckMs = 0

-- Helper: Current timestamp in ms
local function getNowMs()
    if getTimestampMs then
        return getTimestampMs()
    elseif getTimeInMillis then
        return getTimeInMillis()
    end
    return os.time() * 1000
end

-- Helper: ModData table persistence
local function getGlobalData()
    return ModData.getOrCreate("AnimalStasisFix")
end

-- Helper: Retrieve all living animals for a zone (connected pen + hutches)
local function getLivingAnimalsInZone(zone)
    local animals = {}
    if not zone then return animals end

    local connected = zone:getAnimalsConnected()
    if connected then
        -- Add animals currently sheltered inside connected hutches
        local hutches = zone:getHutchsConnected()
        if hutches then
            for i = 0, hutches:size() - 1 do
                local hutch = hutches:get(i)
                if hutch and hutch.getAnimalInside then
                    local map = hutch:getAnimalInside()
                    if map and not map:isEmpty() then
                        pcall(function()
                            connected:addAll(map:values())
                        end)
                    end
                end
            end
        end

        -- Collect living animals from the Java ArrayList
        for i = 0, connected:size() - 1 do
            local animal = connected:get(i)
            if animal and not animal:isDead() then
                table.insert(animals, animal)
            end
        end
    end

    return animals
end

-- Helper: Retrieve all connected hutches
local function getHutchesInZone(zone)
    local list = {}
    if not zone then return list end
    local connectedHutches = zone:getHutchsConnected()
    if connectedHutches then
        for i = 0, connectedHutches:size() - 1 do
            local hutch = connectedHutches:get(i)
            if hutch then
                table.insert(list, hutch)
            end
        end
    end
    return list
end

-- Arm stasis when zone begins unloading
local function armZoneStasis(zone, zoneIdStr)
    local animals = getLivingAnimalsInZone(zone)
    local hutches = getHutchesInZone(zone)

    for _, animal in ipairs(animals) do
        animal:setIsInvincible(true)
        local aModData = animal:getModData()
        if aModData then
            aModData.stasisInvincible = true
            aModData.stasisHealth = animal:getHealth()
        end
    end

    for _, hutch in ipairs(hutches) do
        local hModData = hutch:getModData()
        if hModData then
            hModData.stasisHutchDirt = hutch:getHutchDirt()
            hModData.stasisNestBoxDirt = hutch:getNestBoxDirt()
            hModData.stasisActive = true
        end
    end

    local globalData = getGlobalData()
    if not globalData.unloadedZones then globalData.unloadedZones = {} end
    globalData.unloadedZones[zoneIdStr] = true

    AnimalStasisFix.zoneState[zoneIdStr] = {
        state = "UNLOADED",
        isStreamed = false
    }

    if AnimalStasisFix.DEBUG then
        print(string.format("[AnimalStasisFix] Zone '%s' (ID: %s) entered stasis. %d animal(s) protected.",
            tostring(zone:getName()), zoneIdStr, #animals))
    end
end

-- Re-stream buffer activation when zone is reloaded
local function activateZoneBuffer(zone, zoneIdStr)
    -- Refresh zone squares and connect animals
    pcall(function() zone:check() end)

    local animals = getLivingAnimalsInZone(zone)
    local hutches = getHutchesInZone(zone)

    -- Lock invincibility prior to Java's DesignationZone.update tick
    for _, animal in ipairs(animals) do
        animal:setIsInvincible(true)
    end

    -- Clamp hutch dirt to pre-departure baseline
    for _, hutch in ipairs(hutches) do
        local hModData = hutch:getModData()
        if hModData and hModData.stasisActive then
            if hModData.stasisHutchDirt then
                hutch:setHutchDirt(hModData.stasisHutchDirt)
            end
            if hModData.stasisNestBoxDirt then
                hutch:setNestBoxDirt(hModData.stasisNestBoxDirt)
            end
        end
    end

    local now = getNowMs()
    AnimalStasisFix.zoneState[zoneIdStr] = {
        state = "BUFFERING",
        isStreamed = true,
        bufferEndTime = now + AnimalStasisFix.BUFFER_DURATION_MS
    }

    if AnimalStasisFix.DEBUG then
        print(string.format("[AnimalStasisFix] Zone '%s' (ID: %s) reloaded. 5.0s stasis buffer active for %d animal(s).",
            tostring(zone:getName()), zoneIdStr, #animals))
    end
end

-- Complete buffer and enqueue animals for staggered restoration
local function completeZoneBuffer(zone, zoneIdStr)
    -- Final refresh
    pcall(function() zone:check() end)

    local animals = getLivingAnimalsInZone(zone)
    local hutches = getHutchesInZone(zone)

    -- Clear hutch stasis overrides
    for _, hutch in ipairs(hutches) do
        local hModData = hutch:getModData()
        if hModData then
            hModData.stasisActive = nil
            hModData.stasisHutchDirt = nil
            hModData.stasisNestBoxDirt = nil
        end
    end

    -- Enqueue animals into the 1-per-tick queue
    local enqueuedCount = 0
    for _, animal in ipairs(animals) do
        local animalId = animal.getAnimalID and animal:getAnimalID() or animal
        if not AnimalStasisFix.queuedSet[animalId] then
            table.insert(AnimalStasisFix.queue, animal)
            AnimalStasisFix.queuedSet[animalId] = true
            enqueuedCount = enqueuedCount + 1
        end
    end

    local globalData = getGlobalData()
    if globalData.unloadedZones then
        globalData.unloadedZones[zoneIdStr] = nil
    end

    AnimalStasisFix.zoneState[zoneIdStr] = {
        state = "STREAMED",
        isStreamed = true
    }

    if AnimalStasisFix.DEBUG then
        print(string.format("[AnimalStasisFix] Zone '%s' buffer complete. %d animal(s) enqueued for staggered restoration (%d total in queue).",
            tostring(zone:getName()), enqueuedCount, #AnimalStasisFix.queue))
    end
end

-- Core scan implementation
function AnimalStasisFix.checkZonesInternal()
    if not DesignationZoneAnimal or not DesignationZoneAnimal.getAllZones then return end

    local allZones = DesignationZoneAnimal.getAllZones()
    if not allZones then return end

    local now = getNowMs()
    local globalData = getGlobalData()
    local unloadedMap = globalData.unloadedZones or {}

    for i = 0, allZones:size() - 1 do
        local zone = allZones:get(i)
        if zone then
            local zoneIdStr = tostring(zone:getId())
            local isStreamed = zone:isFullyStreamed()
            local currentState = AnimalStasisFix.zoneState[zoneIdStr]

            -- Initialize state from globalData if not yet known in memory
            if not currentState then
                if unloadedMap[zoneIdStr] then
                    currentState = { state = "UNLOADED", isStreamed = false }
                else
                    currentState = { state = isStreamed and "STREAMED" or "UNLOADED", isStreamed = isStreamed }
                end
                AnimalStasisFix.zoneState[zoneIdStr] = currentState
            end

            if not isStreamed then
                -- Zone is currently unloaded / not fully streamed
                if currentState.state ~= "UNLOADED" then
                    armZoneStasis(zone, zoneIdStr)
                end
            else
                -- Zone is fully streamed
                if currentState.state == "UNLOADED" then
                    -- Transition: Unloaded -> Buffering
                    activateZoneBuffer(zone, zoneIdStr)
                elseif currentState.state == "BUFFERING" then
                    -- While buffering, ensure hutch dirt remains clean of random catchup filth
                    local hutches = getHutchesInZone(zone)
                    for _, hutch in ipairs(hutches) do
                        local hModData = hutch:getModData()
                        if hModData and hModData.stasisActive and hModData.stasisHutchDirt then
                            hutch:setHutchDirt(hModData.stasisHutchDirt)
                            if hModData.stasisNestBoxDirt then
                                hutch:setNestBoxDirt(hModData.stasisNestBoxDirt)
                            end
                        end
                    end

                    -- Check if 5-second buffer has elapsed
                    if now >= (currentState.bufferEndTime or 0) then
                        completeZoneBuffer(zone, zoneIdStr)
                    end
                end
            end
        end
    end
end

-- Safe wrapper around scan
function AnimalStasisFix.checkZones()
    local ok, err = pcall(AnimalStasisFix.checkZonesInternal)
    if not ok and AnimalStasisFix.DEBUG then
        print("[AnimalStasisFix] ERROR in checkZones: " .. tostring(err))
    end
end

-- Chunk load event: Throttled evaluation upon chunk entry
local function onChunkLoaded(chunk)
    local now = getNowMs()
    if now - lastChunkCheckMs < 250 then return end
    lastChunkCheckMs = now
    AnimalStasisFix.checkZones()
end

-- OnTick: Staggered 1-animal-per-frame mortality restoration & throttled zone scan
local function onTick()
    -- 1. Restore exactly 1 animal per tick to prevent CPU / server lag spikes
    if #AnimalStasisFix.queue > 0 then
        local animal = table.remove(AnimalStasisFix.queue, 1)
        if animal then
            local animalId = animal.getAnimalID and animal:getAnimalID() or animal
            AnimalStasisFix.queuedSet[animalId] = nil

            if not animal:isDead() then
                animal:setIsInvincible(false)
                local aModData = animal:getModData()
                if aModData then
                    aModData.stasisInvincible = nil
                    aModData.stasisHealth = nil
                end
                if AnimalStasisFix.DEBUG then
                    print(string.format("[AnimalStasisFix] Restored mortality to animal: %s (Remaining in queue: %d)",
                        tostring(animal:getCustomName() or animal:getType()), #AnimalStasisFix.queue))
                end
            end
        end
    end

    -- 2. Throttled scan for streaming changes
    local now = getNowMs()
    if now - AnimalStasisFix.lastScanMs >= AnimalStasisFix.SCAN_INTERVAL_MS then
        AnimalStasisFix.lastScanMs = now
        AnimalStasisFix.checkZones()
    end
end

-- Event registration
Events.OnTick.Add(onTick)
Events.LoadChunk.Add(onChunkLoaded)

print(string.format("[AnimalStasisFix] Initialized v%s successfully.", AnimalStasisFix.VERSION))
