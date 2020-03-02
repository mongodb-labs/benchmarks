-- long queries, optionally inside a stable snapshot
--
-- depending on --long-queries, there can be 0, 1, 2 or 3 concurrent queries
-- 1 snapshot query / minute runs for 2 minutes -- reads 1GB tepid data
-- 1 snapshot query / 5 minutes runs for 10 minutes -- reads 5GB tepid data
-- 1 snapshot query / day -- reads all data

require("common")
-- like MAX_INT, but Lua uses doubles
LARGE_INT = 2^30

function long_init(thread_id)
    if sysbench.opt.long_queries > sysbench.opt.threads + 1 then
        error("You must use more --threads than --long-queries.")
    end
    if sysbench.opt.long_queries > 3 or sysbench.opt.long_queries < 0 then
        error("--long-queries must be a value between 0 and 3")
    end
end


function long_query(thread_id)
    local target_docs = long_gate(thread_id)
    if target_docs == 0 then
        return
    end

    if sysbench.opt.verbosity >= 3 then
        print("long_query(" .. thread_id .. ")")
    end

    local coll_nr = sysbench.rand.uniform(0, sysbench.opt.num_collections)
    local coll = getCollection(coll_nr)
    local sort_key = {_id = -1}
    local pipeline = { {['$sort'] = sort_key}, {['$limit'] = target_docs} }
    if target_docs == LARGE_INT then
        pipeline = { {['$sort'] = sort_key} }
    end
    local result=coll:aggregate(pipeline)
    if result then
        for singleResult in result do
            -- goal is for the cursor to go through 1M docs per 2 minutes
            -- math isn't perfect, this assumes the read itself takes zero time
            usleep(120)
        end
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
    else
        ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
    end
end

function long_gate(thread_id)
    -- each thread creates successively longer queries
    if thread_id > sysbench.opt.long_queries then
        return 0
    end
    if thread_id == 1 then
        return 1000*1000
    elseif thread_id == 2 then
        return 5*1000*1000
    elseif thread_id == 3 then
        return LARGE_INT  -- becomes full collection scan
    end
end