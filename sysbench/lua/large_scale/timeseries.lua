-- TimeseriesWorkload:
--
-- Original requirements from Alex
--
-- 1 collection with 9 indexes created and dropped / second (default is actually per 5 seconds)
--  * HardLimits describe (latency) requirements that must be met. If one fails, test exits immediately.
--  * Inserts, if non-zero, go to the most recent collection, others are read-only
--  * Each of collection lives for an hour
--  * Queries are rarer and larger as collections get older


-- reuse generate_doc()
require("large_scale/load")

-- note that variables are scoped per thread
-- these are mostly useful for thread_id 0
local ts_head = nil
local ts_tail = nil
local ts_range = nil
local ts_docs_per_collection = nil
local ts_last_create = nil
local ts_last_metadata = nil
function ts_init(thread_id)
    if sysbench.opt.ts_create_interval < 1 then
            error("--ts-create-interval must be a positive integer.")
    end
    ts_head = sysbench.opt.num_collections
    ts_tail = sysbench.opt.num_collections
    ts_range = sysbench.opt.ts_num_collections
    ts_last_create = os.time()
    -- in the beginning all threads can compute the metadata state, so we can consider it as if it was read
    ts_last_metadata = os.time()
    if thread_id == 0 then
        init_metadata()
    end

    if ts_range > 0 and thread_id == 0 then
        -- thread_id == 0 doesn't do inserts
        local no_zero = (sysbench.opt.threads - 1) / sysbench.opt.threads
        ts_docs_per_collection = math.floor( no_zero * sysbench.opt.rate * sysbench.opt.ts_create_interval * sysbench.opt.ts_insert_pct / percent_denominator() / 100 )
        local meta_interval = math.floor( sysbench.opt.rate / sysbench.opt.threads * sysbench.opt.ts_create_interval * sysbench.opt.ts_insert_pct / percent_denominator() / 100 )
        print()
        print("Timeseries sub workload:")
        print("--ts-num-collections=" .. sysbench.opt.ts_num_collections .. " and --ts-create-interval=" .. sysbench.opt.ts_create_interval)
        print("Expect approximately " .. ts_docs_per_collection .. " docs inserted per ts collection.")
        print("Inserts / thread / collection = " .. meta_interval .. " (should be >> 1)")
        print()
    end
end

function ts_dispatcher(thread_id)
    if sysbench.opt.verbosity >= 4 then
        print("ts_dispatcher(" .. thread_id .. ")")
    end

    local d = ts_denominator()
    local insert_pct = sysbench.opt.ts_insert_pct / d
    local point_pct = sysbench.opt.ts_point_query_pct / d
    local range_pct = sysbench.opt.ts_range_query_pct / d
    local gate = sysbench.rand.uniform(0, 100)

    if thread_id == 0 and ts_range > 0 then
        drop_and_create()
    elseif os.time() > ts_last_metadata + sysbench.opt.ts_create_interval then
        -- each thread needs to periodically refresh knowledge of the range of collections to target
        get_metadata(thread_id)
    elseif gate <= insert_pct then
        ts_insert(thread_id)
    end
end

-- drop and create collections
--
-- drop oldest ts collection and create new one every ts_create_interval
-- TODO: original design envisioned drop and create as separate actions, each with their own latency SLA.
-- right now I can't easily track their latency percentiles separately anyway so postponing for now.
function drop_and_create()
    if os.time() >= ts_last_create + sysbench.opt.ts_create_interval then
        if sysbench.opt.verbosity >= 4 then
            print("ts drop_and_create()")
        end

        ts_head = ts_head + 1
        -- implicit create collection
        create_indexes(sysbench.opt.collection_name .. ts_head)
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)

        if ts_head - ts_tail >= ts_range then
            drop_coll = getCollection(ts_tail)
            drop_coll:drop()
            ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)
            ts_tail = ts_tail + 1
        end
        ts_last_create = os.time()
        save_metadata()
    end
end

function save_metadata()
    local coll = getCollection("Metadata")
    local doc = {}
    doc['$set'] = {head = ts_head, tail = ts_tail}
    coll:update_one({_id = "ts"}, doc)
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)
end

function init_metadata()
    print("Init timeseries metadata")
    local coll = getCollection("Metadata")
    local doc = {
        _id = "ts",
        head = ts_head,
        tail = ts_tail,
    }
    coll:delete_one({_id = "ts"})
    coll:insert_one(doc)
end

-- generate ids guaranteed to be unique between threads
local ts_id_counter = -1
function ts_id(thread_id)
    ts_id_counter = ts_id_counter + 1
    return thread_id + sysbench.opt.threads * ts_id_counter
end

function get_metadata(thread_id)
    if sysbench.opt.verbosity >= 4 then
        print("ts get_metadata(" .. thread_id .. ")")
    end

    local coll = getCollection("Metadata")
    local doc = coll:find_one({_id = "ts"})
    if doc['head'] > ts_head then
        if sysbench.opt.verbosity >= 4 then
            print("ts get_metadata(" .. thread_id .. "): reset ts_id_counter")
        end
        -- inserts roll over to new collection immediately, start _id's from scratch
        ts_id_counter = -1
    end
    ts_head = doc['head']
    ts_tail = doc['tail']
    ts_last_metadata = os.time()
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)
end

function ts_denominator()
    -- like percent_denominator() but internal to timeseries.lua
    local d = (
        sysbench.opt.ts_insert_pct + 
        sysbench.opt.ts_point_query_pct + 
        sysbench.opt.ts_range_query_pct) / 100
    -- avoid division by zero
    if d == 0 then
        return 1
    else
        return d
    end
end

-- insert
function ts_insert(thread_id)
    if sysbench.opt.verbosity >= 4 then
        print("ts_insert(" .. thread_id .. ") " .. ts_head .. "-" .. ts_tail)
    end
    if ts_head > ts_tail then
        if sysbench.opt.verbosity >= 4 then
            print("ts_insert() - do insert")
        end

        local coll = getCollection(ts_head)
        local doc = generate_doc(ts_id(thread_id))
        doc['thread_id'] = thread_id
        if coll:insert_one(doc) then
            ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_WRITE)
        else
            ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
        end
    end
end

-- queries

function reverse_pareto(min, max)
    -- pareto function where max is the most likely value
    return max - sysbench.rand.pareto(min, max) + 1
end

function ts_point_query(thread_id)
    -- target the most recent values of the most recent collection.
    local coll_nr = reverse_pareto(ts_tail, ts_head)
    local coll = getCollection(coll_nr)
    local match_id = reverse_pareto(1, ts_docs_per_collection * 0.8)
    -- _id is a bit sparse, use <= to ensure we return exactly one doc
    -- note that for the newest collection inserts are still happening, so this also takes care
    -- of the situation where the higher _id's simply don't exist yet.
    if coll:find_one({_id = {['$lte'] = match_id}}) then
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
    else
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
    end
end

function ts_range_query(thread_id)
    -- target the most recent values of the most recent collection.
    local coll_nr = reverse_pareto(ts_tail, ts_head)
    local coll = getCollection(coll_nr)
    local match_id = reverse_pareto(1, ts_docs_per_collection * 0.8)
    local match = {_id = {['$lte'] = match_id} }
    local sort_key = {_id = -1}
    local length = 1000
    local pipeline = { { ['$match'] = match}, {['$sort'] = sort_key}, {['$limit'] = length} }
    local result=coll:aggregate(pipeline)
    if result then
        for singleResult in result do
        end
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
    else
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
    end
end
