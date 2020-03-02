#!/usr/bin/env sysbench

-- A large long running workload
-- Based on this spec from Alex Gorrod https://github.com/henrikingo/genny/commit/942dcfa92eaf946a84a588e12c00859e330993ef
-- In turn based on this requirements doc: https://docs.google.com/document/d/1Gut_7zhm4Kc9emG7Y4xW2iDapiYGGu6eYFcK4HU7OeQ/edit#heading=h.rawojqixilb

require("common")
require("large_scale/load")
require("large_scale/timeseries")
require("large_scale/long_queries")

ffi.cdef[[
void sb_counter_inc(int thread_id, sb_counter_type type);
]]

-- Options specific to this test

-- Example prepare: ./large_scale.lua --parallel-prepare --num-threads=10 --max-requests=10
-- Example run:     ./large_scale.lua --rate=10000 --num-threads=500


sysbench.cmdline.options["parallel-prepare"] = {"Hack: Use 'large_scale.lua run --parallel-prepare' instead of regular prepare.", false}
sysbench.cmdline.options["loader-sleep"] = {"Loader thread should sleep N seconds before next batch insert.", 0}
sysbench.cmdline.options["num-collections"] = {"How many collections to create. (Each contains num-docs.)", 10*1000}
sysbench.cmdline.options["set-oplog-size"] = {"Set (override) oplog size before starting. Megabytes. 0 = don't set.", 0}
sysbench.cmdline.options["indexes-last"] = {"Create indexes after inserting.", "no"}


-- default = 1% of collections
sysbench.cmdline.options["update-collections-pct"] = {"What percentage of collections receive updates.", 1}
sysbench.cmdline.options["update-pct"] = {"Percentage of --rate that are updates.", 50}
-- default = 10% of collections
sysbench.cmdline.options["query-collections-pct"] = {"What percentage of collections receive queries.", 10}
sysbench.cmdline.options["query-pct"] = {"Percentage of --rate that are queries.", 50}

-- timeseries
sysbench.cmdline.options["ts-num-collections"] = {"How many collections the timeseries sub-workload creates on top of the regular ones.", 0}
sysbench.cmdline.options["ts-create-interval"] = {"Seconds between dropping one ts collection and creating a new one.", 5}
sysbench.cmdline.options["ts-insert-pct"] = {"Percentage of --rate that are inserts of the timeseries workload.", 0}
sysbench.cmdline.options["ts-point-query-pct"] = {"Percentage of --rate that are point queries of the timeseries workload.", 0}
sysbench.cmdline.options["ts-range-query-pct"] = {"Percentage of --rate that are range queries of the timeseries workload.", 0}

-- long queries
sysbench.cmdline.options["long-queries"] = {"Number of concurrent long queries. (0 .. 3)", 0}
sysbench.cmdline.options["long-queries-trx"] = {"Do long query inside a transaction (TODO)", false}



function prepare()
    if sysbench.opt.set_oplog_size > 0 then
        set_oplog_size(sysbench.opt.set_oplog_size)
    end
    parallel_prepare(0, 1)
end

function cleanup()
    local db = getDB()
    db:drop_database()
end

local parallel_prepare_done = false
function thread_init(thread_id)
    parallel_prepare_done = false
    thread_time_keepalive = os.time()
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(sysbench.opt)
    end
    if sysbench.opt.set_oplog_size > 0 and thread_id == 0 then
        set_oplog_size(sysbench.opt.set_oplog_size)
    end
    ts_init(thread_id)
end

function thread_done(thread_id)
end

local thread_time_keepalive = os.time()
function event(thread_id)
    if sysbench.opt.parallel_prepare then
        if parallel_prepare_done then
            return
        end
        parallel_prepare(thread_id, sysbench.opt.threads)
        parallel_prepare_done = true
        return
    end
    if os.time() > thread_time_keepalive + 5*60 and thread_id == 0 then
        print("It's " .. os.date("%Y-%m-%dT%H:%M:%S") .. " and I'm still working...")
        thread_time_keepalive = os.time()
    end

    -- scale to 100%
    local d = percent_denominator()
    local update_pct = sysbench.opt.update_pct / d
    local query_pct = sysbench.opt.query_pct / d
    local ts_pct = (sysbench.opt.ts_insert_pct + sysbench.opt.ts_point_query_pct + sysbench.opt.ts_range_query_pct ) / d
    local gate = sysbench.rand.uniform(0, 100)
    if thread_id == 0 and sysbench.opt.ts_num_collections > 0 then
        ts_dispatcher(thread_id)
    elseif thread_id > 0 and thread_id <= sysbench.opt.long_queries then
        long_query(thread_id)
    elseif gate <= update_pct then
        update(thread_id)
    elseif gate <= update_pct + query_pct then
        query(thread_id)
    else
        ts_dispatcher(thread_id)
    end
end

function percent_denominator()
    -- the options for different query types should total 100, but in case they don't we scale them to 100 anyway
    local d = (sysbench.opt.update_pct + 
        sysbench.opt.query_pct + 
        sysbench.opt.ts_insert_pct + 
        sysbench.opt.ts_point_query_pct + 
        sysbench.opt.ts_range_query_pct) / 100
    if d == 0 then
        return 1
    else
        return d
    end
end

-- individual operations

function update(thread_id)
    if sysbench.opt.verbosity >= 4 then
        print("update(" .. thread_id .. ")")
    end
    local coll = getCollection(sysbench.rand.uniform(0, sysbench.opt.update_collections_pct))
    local match = {y = sysbench.rand.gaussian(0, 200)}
    local set = {['$inc'] = {x = 1}}
    if coll:update_one(match, set) then
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_WRITE)
    else
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
    end
end

function query(thread_id)
    if sysbench.opt.verbosity >= 4 then
        print("query(" .. thread_id .. ")")
    end
    local coll = getCollection(sysbench.rand.uniform(0, sysbench.opt.query_collections_pct))
    local match = {y = sysbench.rand.gaussian(0, 200)}
    local length = 20
    local pipeline = { { ['$match'] = match}, {['$limit'] = length} }
    local result=coll:aggregate(pipeline )
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(result)
    elseif result then
        -- just read the cursor to end
        for singleResult in result do
        end
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
    else
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
    end
end

-- reporting api
function sysbench.hooks.report_intermediate(stat)
    common_report_intermediate(stat)
end

function sysbench.hooks.report_cumulative(stat)
    -- The "results" section contains the results that are supposed to be "interesting" for this
    -- benchmark. The key names should be useful as labels.
    local my_results = {}
    if sysbench.opt.parallel_prepare then
        my_results["batches_per_sec"] = stat.writes / stat.time_total
        my_results["docs_per_sec"] = stat.writes / stat.time_total * sysbench.opt.batch_size
    else
        my_results["latency_" .. sysbench.opt.percentile .. "_ms"] = stat.latency_pct * 1000
        -- TODO these aggregates stats really mix a lot of different queries and ops together.
        -- good example of the need to add ability to have metrics per operation type
        my_results["reads_per_sec"] = stat.reads / stat.time_total
        my_results["writes_per_sec"] = stat.writes / stat.time_total
        my_results["other_per_sec"] = stat.other / stat.time_total
        my_results["ops_per_sec"] = (stat.reads + stat.writes + stat.other) / stat.time_total
    end

    common_report_cumulative(stat, my_results)
end
