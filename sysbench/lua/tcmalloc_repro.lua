#!/usr/bin/env sysbench

-- Attempt to repro SERVER-34027
-- On a large server tcmalloc had 66 GB of memory fragmentation over 3M spans.
-- When decommitting all that back to OS it stalls mongod for 30 seconds.
--
-- Based on the diagnostic.data provided, I decomposed the workload into:
-- ops
--   8750 queries/sec
--   400 getmore/sec
--   The node is a secondary, so all writes are from replication:
--   250 inserts/sec, with occasional plateaus at 500 inserts/sec
--   450 updates/sec, with variation up to 690 updates/sec
--   A TTL index deletes ~12k documents every minute.
--   30 deletes/sec outside of this.
--   15 average documents returned per query ~ 140k documents / sec. 
-- query planner
--   280k scanned index keys / sec (2:1 hit rate), with occasional spike at 3M
--   140k scanned objects / sec, but with occasional spike at 3M.
--   10 scanAndOrder per second
--   Most of above 280k scanned is in wt cursor next, but an occasional spike to 2k for cursor prev can be seen.
-- net
--   1600 connections / sec
--   34 MB/sec physicalBytesOut. From this we can estimate an average 255 bytes per document.
--       We don't know anything about distribution. I assume it matters, so I will generate documents of various size.
-- wt
--   200GB in wt cache, at 80% fill ratio.
--   6k blocks read / sec, all of which is also in "application threads page read".
--   11k cache eviction thread evicting pages


require("common")

ffi.cdef[[
void sb_counter_inc(int thread_id, sb_counter_type type);
]]


-- Please use the following common sysbench options:  (Can't set defaults from here.)
--       'sysbench tcmalloc_repro.lua  --threads=1600

-- Options specific to this test
sysbench.cmdline.options["parallel-prepare"] = {"Hack: Use 'tcmalloc_repro.lua run --parallel-prepare' instead of regular prepare.", false}
sysbench.cmdline.options["doc-size-min"] = {"Minimum document size. (60 or higher)", 60}
sysbench.cmdline.options["doc-size-max"] = {"Maximum document size.", 1400}
sysbench.cmdline.options["doc-size-smudge"] = {"Smudge factor: Adds a uniform variable to the otherwise pareto distributed size. Value between 0 (pareto-only) and 1 (uniform-only).", 0.05}

function prepare()
    parallel_prepare(0, 1)
end

local thread_time_keepalive = 0
function parallel_prepare (thread_id, num_threads)
    -- A bit odd way to insert docs, because I want to insert them roughly in increasing order even with parallel threads.
    -- As if it was an IOT workload.
    local my_first_id = thread_id + 1
    local my_last_id = sysbench.opt.num_docs - num_threads + thread_id

    local coll = getCollection()
    if thread_id == 0 then
        print("thread_id 0 creating indexes...")
        -- some mongorover error simply from calling db:command() now. Really would need a bit of a refresh on mongorover...
        -- the index does get created though.
        pcall(create_indexes, sysbench.opt.collection_name)
        print("thread_id 0 Finished creating indexes.")
    end

    local id = my_first_id
    while id <= my_last_id do
        -- print progress every 5 min. Among other things to avoid timeouts.
        if os.time() > thread_time_keepalive + 5*60 or sysbench.opt.verbosity >= 4 then

            print("thread_id " .. thread_id .. " at id = " .. id .. " / " .. my_last_id)
            thread_time_keepalive = os.time()
        end

        local doc = generate_doc(id)
        doc['ts'] = simulated_ts(id)
        local result = coll:insert_one(doc)
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_WRITE)
        if sysbench.opt.verbosity >= 5 then
            pretty.dump(result)
        end

        id = id + num_threads
    end
    -- remainder, covers all threads
    if thread_id == 0 then
        local id = sysbench.opt.num_docs - sysbench.opt.num_docs % num_threads + 1
        while id <= sysbench.opt.num_docs do
            if sysbench.opt.verbosity >= 4 then
                print("thread_id " .. thread_id .. " at id = " .. id .. " / " .. sysbench.opt.num_docs)
            end

            local doc = generate_doc(id)
            doc['ts'] = simulated_ts(id)
            local result = coll:insert_one(doc)
            ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_WRITE)
            if sysbench.opt.verbosity >= 5 then
                pretty.dump(result)
            end

            id = id + 1
        end
    end
end

function create_indexes(coll_name)
    local db = getDB()
    local index_name = "simulated_ttl_idx"
    local index_definition = {ts = 1}
    db:command("createIndexes", coll_name, { indexes = {{ key = index_definition, name = index_name }}})
end

-- Available random distributions:
-- https://github.com/akopytov/sysbench/blob/master/src/lua/internal/sysbench.rand.lua
-- sysbench.rand.default() uses what is set with --rand-type=pareto (default: special)

-- Like MAX_INT, but Lua only has doubles
local LARGE_INT = 999999
function generate_doc(id)
    -- Size of the other fields
    other_bytes = 60
    -- "Smudged" pareto distribution: Add together large pareto + small uniform range
    local start = math.max(sysbench.opt.doc_size_min - other_bytes, 0)
    local stop = math.max(sysbench.opt.doc_size_max - other_bytes, 0)
    local uniform_part = sysbench.opt.doc_size_smudge * (stop - start)
    local uniform = sysbench.rand.uniform(0, uniform_part)
    local pareto = sysbench.rand.pareto(start, stop-uniform_part)
    local str_length = uniform + pareto
    string_format = string.rep("@", str_length)
    return {_id = id,
            ts = os.time(),
            i = sysbench.rand.uniform(0, LARGE_INT),
            j = id % 250,
            a = sysbench.rand.string(string_format),
    }
end

-- For the prepare phase we want to simulate the time field so that there are approx 250 docs / sec.
-- They will be generated into the past wrt start of the sysbench execution. This leaves a gap between prepare phase and run phase, which we ignore.
local start_ts = os.time()
function simulated_ts(id)
    local reverse_id = sysbench.opt.num_docs - id
    local ts = start_ts - math.floor(reverse_id / 250)
    return ts
end


-- End of prepare related functions

-- sysbench api (entry point functions)



function cleanup()
    local db = getDB()
    db:drop_database()
end

local parallel_prepare_done = false

-- A few operations happen 1 / minute, so use one designated thread and timer
local ttl_thread_id = 0
local ttl_last_ts = nil
local ttl_last_run = os.time()
local spike3m_thread_id = 1
local spike3m_last_time = os.time()
local next_id = 1
-- should happen a few hundred / sec
local insert_thread_id_min = 2
local insert_thread_id_max = 4
local num_insert_threads = insert_thread_id_max - insert_thread_id_min + 1
function thread_init(thread_id)
    -- prepare related variables
    parallel_prepare_done = false
    thread_time_keepalive = 0
    start_ts = os.time()

    -- run related variables
    ttl_last_ts = get_ts_floor(thread_id)
    ttl_last_run = os.time()
    spike3m_last_time = os.time()
    next_id = get_start_id(thread_id)
end

function thread_done(thread_id)
end

function event(thread_id)
    if sysbench.opt.parallel_prepare then
        if parallel_prepare_done then
            return
        end
        parallel_prepare(thread_id, sysbench.opt.threads)
        parallel_prepare_done = true
        return
    end

    dispatcher(thread_id)
end

-- functions called from thread_init()

-- query the lowest timestamp. simulate_ttl() will start deleting from there.
function get_ts_floor(thread_id)
    if thread_id == ttl_thread_id then
        local ts_floor = 1
        local coll = getCollection()
        local sort_key = {ts = 1}
        local pipeline = { {['$sort'] = sort_key}, {['$limit'] = 1 } }
        local result=coll:aggregate(pipeline)
        for doc in result do
            ts_floor = doc['ts']
        end
        print("thread_id " .. thread_id .. " ttl ts initialized to " .. ts_floor)
        return ts_floor
    end
end

-- query the highest _id and add thread_id so that each thread starts at its own offset
function get_start_id(thread_id)
    if thread_id >= insert_thread_id_min and thread_id <= insert_thread_id_max then
        local start_id = 1
        local coll = getCollection()
        local sort_key = {_id = -1}
        local pipeline = { {['$sort'] = sort_key}, {['$limit'] = 1 } }
        local result=coll:aggregate(pipeline)
        for doc in result do
            start_id = doc['_id'] + thread_id
        end
        print("thread_id " .. thread_id .. " _id initialized to " .. start_id)
        return start_id
    end
end

-- functions called from event()

function dispatcher(thread_id)
    if thread_id == spike3m_thread_id and os.time() > spike3m_last_time + 51 then
        spike3m_read(thread_id)
        spike3m_last_time = os.time()
    elseif thread_id == ttl_thread_id and os.time() > ttl_last_run + 60 then
        simulate_ttl(thread_id)
        ttl_last_run = os.time()
    elseif thread_id >= insert_thread_id_min and thread_id <= insert_thread_id_max then
        insert(thread_id)
    else
        read_dispatcher(thread_id)
    end
end

function simulate_ttl(thread_id)
    local coll = getCollection()
    -- delete 60 sec range of the oldest records
    local filter = { ts = { ['$lte'] = ttl_last_ts + 60 } }
    print("thread_id " .. thread_id .. " doing simulated ttl deletes now...")
    result = coll:delete_many(filter)
    if result then
        for i=1, result.deleted_count do
            ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_WRITE)
        end
    else
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
    end
    ttl_last_ts = ttl_last_ts + 60
    print("thread_id " .. thread_id .. " finished simulated ttl deletes.")
end

-- insert a single doc
function insert(thread_id)
    local coll = getCollection()
    local doc = generate_doc(next_id)
    if coll:insert_one(doc) then
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_WRITE)
    else
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
    end
    next_id = next_id + num_insert_threads
end

function read_dispatcher(thread_id)
    -- randomly select read operation. See workload description on top.
    local token = sysbench.rand.uniform(0, 100)
    if token < 0.1 then
        scanAndOrder() -- also does getMore
    elseif token < 5 then
        getMore()
    elseif token < 90 then
        find()
    end
end

-- simple find to get a small amount of docs.
function find()
    local coll = getCollection()
    local now = os.time()
    local past = now - sysbench.rand.uniform(0, 60*60)
    -- There will be ~250 docs per second - more than we want. So we also match on j to limit to a handful.
    local j = sysbench.rand.uniform(0, 250)
    local result=coll:find({ts = {['$lte'] = past, ['$gt'] = past-1}, j = {['$lt'] = j + 4, ['$gte'] = j} })
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(result)
    else
        -- just read the cursor to end
        for singleResult in result do
            ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)
        end
    end
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
end

-- read a recent time interval, then order by i
function scanAndOrder()
    local coll = getCollection()
    local now = os.time()
    local past = now - sysbench.rand.uniform(0, 60*60)
    local length = 4
    local match = {ts = {['$lte'] = past, ['$gt'] = past-length}}
    local sort_key = {i = 1}
    local pipeline = { { ['$match'] = match}, {['$sort'] = sort_key} }
    local result=coll:aggregate(pipeline, {allowDiskUse=true} )
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(result)
    else
        -- just read the cursor to end
        for singleResult in result do
            ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)
        end
    end
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
end

-- read a recent time interval, no sort
function getMore()
    local coll = getCollection()
    local now = os.time()
    local past = now - sysbench.rand.uniform(0, 60*60)
    local length = sysbench.rand.uniform(0, 60)
    local match = {ts = {['$lte'] = past, ['$gt'] = past-length}}
    local pipeline = { { ['$match'] = match} }

    -- For half the queries, add a filter on i
    local coin = sysbench.rand.uniform(0, 100)
    if coin > 50 then
        pipeline[1]['$match']['i'] = {['$gt'] = LARGE_INT/2}
    end

    local result=coll:aggregate(pipeline)
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(result)
    else
        -- just read the cursor to end
        for singleResult in result do
            ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)
        end
    end
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
end

-- Once per minute, read 3 million documents
function spike3m_read(thread_id)
    local coll = getCollection()
    local pipeline = { { ['$sort'] = {ts = -1} }, { ['$limit'] = 3*1000*1000 } }
    print("thread_id " .. thread_id .. " querying 3 million records now...")
    local result=coll:aggregate(pipeline)
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(result)
    else
        -- just read the cursor to end
        for singleResult in result do
            ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)
        end
    end
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
    print("thread_id " .. thread_id .. " finished querying 3 million records.")
end




-- reporting api
function sysbench.hooks.report_intermediate(stat)
    common_report_intermediate(stat)
end

function sysbench.hooks.report_cumulative(stat)
    -- The "results" section contains the results that are supposed to be "interesting" for this
    -- benchmark. The key names should be useful as labels.
    my_results = {}
    if sysbench.opt.parallel_prepare then
        my_results["inserts_per_sec"] = stat.writes / stat.time_total
    else
        my_results["latency_" .. sysbench.opt.percentile .. "_ms"] = stat.latency_pct * 1000
        my_results["reads_per_sec"] = stat.reads / stat.time_total
        my_results["writes_per_sec"] = stat.reads / stat.time_total
        -- sysbench adds other to stat.ops, which is wrong in this workload. compute our own.
        my_results["ops_per_sec"] = stat.reads / stat.time_total + stat.writes / stat.time_total
        my_results["docs_per_sec"] = stat.other / stat.time_total
    end
    if stat.errors > 0 then
        my_results["errors_total"] = stat.errors
    end

    common_report_cumulative(stat, my_results)
end
