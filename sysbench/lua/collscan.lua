#!/usr/bin/env sysbench

-- Single thread doing a full collection scan.
-- Reports total latency and also docs / sec.

require("common")

ffi.cdef[[
void sb_counter_inc(int thread_id, sb_counter_type type);
]]


-- Options specific to this test
sysbench.cmdline.options["project"] = {"Only return given field.", ""}


function prepare ()
    print("prepare: Not implemented. Use existing collection of use prepare from high_thread_count.lua")
end

function cleanup()
    print("prepare: Not implemented. Use existing collection of use prepare from high_thread_count.lua")
end

function thread_init(thread_id)
    coll = getCollection()
    key = {}
    project = {}
    if sysbench.opt.project == "" then
        project = {}
    else
        project[sysbench.opt.project] = 1
    end
end

function thread_done(thread_id)
end

function event(thread_id)
    local cursor = coll:find(key, project)
    for doc in cursor do
        if sysbench.opt.verbose > 3 then
            pretty.dump(doc)
        end
        -- OTHER counter is used for docs/sec in this test
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)
    end
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
end



function sysbench.hooks.report_intermediate(stat)
    common_report_intermediate(stat)
end

function sysbench.hooks.report_cumulative(stat)
    -- my_results contains the results that are supposed to be "interesting" for this
    -- benchmark. The key names should be useful as lables.
    my_results = {}
    my_results["total_time_ms"] = stat.time_total * 1000
    my_results["latency_" .. sysbench.opt.percentile .. "_ms"] = stat.latency_pct * 1000
    my_results["docs_per_sec"] = stat.other / stat.time_total
    common_report_cumulative(stat, my_results)
end
