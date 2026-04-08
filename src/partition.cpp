#include "backend_internal.hpp"
#include "dispatch.hpp"
#include <cstring>

// ---- PartitionImpl ----

template <int N>
void PartitionImpl<N>::init(typename PartitionTraits<N>::ProxyType proxy, std::array<int, N> idx,
                            CProxy_ArrayDAGGroup dag_proxy_) {
    thisProxy = proxy;
    index = idx;
    dag_proxy = dag_proxy_;
#ifdef USE_NVIDIA
    cudaStreamCreate(&compute_stream_raw);
    cudaStreamCreate(&comm_stream_raw);
    compute_exec = Kokkos::Cuda(compute_stream_raw);
    comm_exec = Kokkos::Cuda(comm_stream_raw);
#endif
    auto* dag_group = static_cast<ArrayDAGGroup*>(dag_proxy_.ckLocalBranch());
    executor = new ArrayDAGExecutorND<N>(dag_group, this);
    // Skip to the epoch where this partition was first created.
    // start_epoch - 1 so that next_epoch() advances to start_epoch.
    int se = dag_group->partition_grid[N].start_epoch;
    if (se > 0)
        executor->epoch = se - 1;
    ChareIndex<N> ci;
    for (int d = 0; d < N; ++d)
        ci.idx[d] = index[d];
    proxy_at<N>(thisProxy, ci).run();
}

template <int N>
PartitionImpl<N>::~PartitionImpl() {
    delete executor;
    for (auto& [name, arr] : arrays)
        delete arr;
    for (auto& [key, bucket] : free_arrays)
        for (auto* arr : bucket)
            delete arr;
#ifdef USE_NVIDIA
    cudaStreamDestroy(compute_stream_raw);
    cudaStreamDestroy(comm_stream_raw);
#endif
}

template <int N>
int PartitionImpl<N>::create(ArrayRegion<N>* region, int name, DType dtype,
                             const ArrayDecomp<N>& decomp) {

    auto nd_idx = this->nd_index();
    auto* dag_group = static_cast<ArrayDAGGroup*>(dag_proxy.ckLocalBranch());

    std::array<int, N> global_shape{};
    std::array<int, N> local_sizes{};
    std::array<int, N> zeros{};
    std::array<int, N> steps{};
    zeros.fill(0);

    for (int d = 0; d < N; ++d) {
        global_shape[d] = region->size(d);
        steps[d] = region->step[d];
    }

    ArrayDAGGroup::ArrayMetadata live_meta = {};
    live_meta.ndims = N;
    live_meta.global_shape = {0, 0, 0};
    live_meta.offset = {0, 0, 0};
    live_meta.tile = decomp.tile;
    for (int d = 0; d < N; ++d) {
        live_meta.global_shape[d] = global_shape[d];
        live_meta.offset[d] = decomp.offset[d];
    }
    dag_group->live_array_meta[name] = live_meta;

    auto local_region = decomp.chare_region_local(nd_idx);
    for (int d = 0; d < N; ++d) {
        local_sizes[d] = local_region.size(d);
        if (local_sizes[d] <= 0)
            return name;
    }

    ArrayRegion<N> chare_region(zeros, local_sizes, steps);
    auto old_it = arrays.find(name);
    if (old_it != arrays.end()) {
        delete old_it->second;
        arrays.erase(old_it);
    }

    // Try to reuse a retired buffer instead of allocating fresh memory.
    int local_total = 1;
    for (int d = 0; d < N; ++d)
        local_total *= local_sizes[d];
    CTArrayBase<N>* reused = try_reuse(dtype, local_total);
    if (reused) {
        reused->name = name;
        reused->region = chare_region;
        reused->global_shape = global_shape;
        reused->decomp = decomp;
        reused->global_size = 1;
        for (int d = 0; d < N; ++d)
            reused->global_size *= global_shape[d];
        arrays[name] = reused;
        return name;
    }

    switch (dtype) {
    case DType::FLOAT32:
        arrays[name] = new Array<N, float>(chare_region, global_shape, name, decomp);
        break;
    case DType::FLOAT64:
        arrays[name] = new Array<N, double>(chare_region, global_shape, name, decomp);
        break;
    case DType::INT32:
        arrays[name] = new Array<N, int32_t>(chare_region, global_shape, name, decomp);
        break;
    case DType::INT64:
        arrays[name] = new Array<N, int64_t>(chare_region, global_shape, name, decomp);
        break;
    }
    return name;
}

template <int N>
void PartitionImpl<N>::run() {
    // If a DAG is currently executing (not all nodes done), ignore this
    // stale run() from the NONE polling loop.  The DAG completion callback
    // will re-invoke run() when the DAG finishes.
    if (executor->dag != nullptr &&
        executor->dag->num_nodes_done < executor->dag->num_nodes) {
        return;
    }


#ifndef NDEBUG
    // Print cumulative communication volume at the end of each DAG epoch
    if (executor->dag != nullptr &&
        executor->dag->num_nodes_done >= executor->dag->num_nodes) {
        DBG_PRINT("[PE %d] Partition<%d> chare (%d",
                  CkMyPe(), N, index[0]);
        for (int d = 1; d < N; ++d)
            CkPrintf(",%d", index[d]);
        CkPrintf("): epoch %d done, cumulative comm_bytes_sent=%lld\n",
                 executor->epoch, (long long)comm_bytes_sent);
    }
#endif

    ChareIndex<N> ci;
    for (int d = 0; d < N; ++d)
        ci.idx[d] = index[d];

    EpochType type = executor->next_epoch();
    DBG_PRINT("[PE %d] Partition<%d> chare %d: run() epoch=%d type=%s\n",
              CkMyPe(), N, index[0], executor->epoch,
              type == EpochType::DAG ? "DAG" : type == EpochType::GET ? "GET" : "NONE");
    switch (type) {
    case EpochType::DAG:
        executor->execute_dag(
            CkCallback(PartitionTraits<N>::CkIndexType::run(), proxy_at<N>(thisProxy, ci)));
        break;
    case EpochType::GET:
        process_get(executor->epoch);
        proxy_at<N>(thisProxy, ci).run();
        break;
    case EpochType::NONE:
        break;
    }
}

#ifdef USE_KOKKOS
template <int N>
void PartitionImpl<N>::receive_data(int& node_id, int& input_index, int& name, int& ndims,
                                    int*& region_data, int64_t& size, char*& data,
                                    CkDeviceBufferPost* devicePost) {
    // Post entry method: allocate device buffer for incoming GPU data
    data = static_cast<char*>(Kokkos::kokkos_malloc<DeviceSpace>(size));
}

template <int N>
void PartitionImpl<N>::send_complete(int send_id) {
    auto it = pending_sends.find(send_id);
    if (it != pending_sends.end()) {
        Kokkos::kokkos_free<DeviceSpace>(it->second);
        pending_sends.erase(it);
    }
}
#endif

template <int N>
void PartitionImpl<N>::receive_data(int node_id, int input_index, int name, int ndims,
                                    int* region_data, int64_t size, char* data) {
    PendingComm<N>& comm = executor->pending[node_id];

#ifdef USE_KOKKOS
    // data is already a device pointer (allocated in post method, filled by GPU messaging)
    char* buf = data;
#else
    char* buf = new char[size];
    memcpy(buf, data, size);
#endif

    // Unpack region from region_data: [start0, stop0, step0, start1, stop1, step1, ...]
    std::array<int, N> start, stop, step;
    for (int d = 0; d < N; ++d) {
        start[d] = region_data[d * 3 + 0];
        stop[d] = region_data[d * 3 + 1];
        step[d] = region_data[d * 3 + 2];
    }
    ArrayRegion<N> region(start, stop, step);

    comm.remote_buffers[input_index].push_back({buf, region, size});

    if (comm.incremental) {
        // Incremental mode: compute with this panel immediately
        executor->on_matmatmul_receive(node_id, input_index);
    }

    if (comm.expected_msgs > 0) {
        comm.expected_msgs--;
        if (comm.expected_msgs == 0)
            executor->on_comm_done(node_id);
    } else {
        comm.expected_msgs--;
    }
}

template <int N>
void PartitionImpl<N>::comm_done(int node_id) {
    executor->on_comm_done(node_id);
}

template <int N>
void PartitionImpl<N>::reduce_result(CkReductionMsg* msg) {
    if constexpr (N == 1) {
        // Extract metadata and summed value from the custom reducer's output
        ReduceContrib result;
        memcpy(&result, msg->getData(), sizeof(ReduceContrib));
        int node_id = result.node_id;
        int result_name = result.result_name;
        DType dt = static_cast<DType>(result.dtype_int);

        // Create the scalar result array (size 1) on chare 0
        if (arrays.find(result_name) == arrays.end()) {
            auto* dag_group = static_cast<ArrayDAGGroup*>(dag_proxy.ckLocalBranch());
            ArrayDecomp<1> out_decomp = dag_group->array_meta[result_name].decomp<1>();
            std::array<int, 1> out_start = {0};
            std::array<int, 1> out_stop = {1};
            std::array<int, 1> out_step = {1};
            std::array<int, 1> out_gs = {1};
            ArrayRegion<1> out_region(out_start, out_stop, out_step);
            switch (dt) {
            case DType::FLOAT32:
                arrays[result_name] =
                    new Array<1, float>(out_region, out_gs, result_name, out_decomp);
                break;
            case DType::FLOAT64:
                arrays[result_name] =
                    new Array<1, double>(out_region, out_gs, result_name, out_decomp);
                break;
            case DType::INT32:
                arrays[result_name] =
                    new Array<1, int32_t>(out_region, out_gs, result_name, out_decomp);
                break;
            case DType::INT64:
                arrays[result_name] =
                    new Array<1, int64_t>(out_region, out_gs, result_name, out_decomp);
                break;
            }
        }

        // Store the reduced value
        int elem_size = dtype_size(dt);
#ifdef USE_KOKKOS
        Kokkos::deep_copy(
            Kokkos::View<char*, DeviceSpace, Kokkos::MemoryTraits<Kokkos::Unmanaged>>(
                static_cast<char*>(arrays[result_name]->device_data_ptr()), elem_size),
            Kokkos::View<char*, HostSpace, Kokkos::MemoryTraits<Kokkos::Unmanaged>>(
                result.value, elem_size));
#else
        memcpy(arrays[result_name]->data_ptr(), result.value, elem_size);
#endif

        DBG_PRINT("[1D Chare %d] reduce_result: node=%d result_name=%d stored\n",
                  index[0], node_id, result_name);

        delete msg;
        executor->node_finished(node_id);
    } else {
        delete msg;
    }
}

template <int N>
void PartitionImpl<N>::process_get(int epoch) {

    auto nd_idx = this->nd_index();

    DBG_PRINT("Partition<%d> processing get for epoch %d\n", N, epoch);
    auto* req = executor->group->get_get_request(N, epoch);
    if (!req) {
        CkAbort("No get request found for epoch %d", epoch);
        return;
    }
    int name = req->name;
    if (arrays.find(name) == arrays.end()) {
        // This partition doesn't own any portion of the array (e.g. the array
        // is smaller than the chare grid).  Nothing to contribute.
        DBG_PRINT("Partition<%d>: array %d not local, skipping get for epoch %d\n", N, name,
                  epoch);
        return;
    }

    arrays[name]->copyToHost();

    {
        CTArrayBase<N>* arr = arrays[name];
        int esz = arr->elem_size();
        auto global_region = arr->decomp.chare_region_global(nd_idx);

        std::array<int64_t, N> global_strides;
        global_strides[N - 1] = 1;
        for (int d = N - 2; d >= 0; --d)
            global_strides[d] = global_strides[d + 1] * (int64_t)arr->global_shape[d + 1];

        std::array<int64_t, N> local_strides;
        local_strides[N - 1] = 1;
        for (int d = N - 2; d >= 0; --d)
            local_strides[d] = local_strides[d + 1] * (int64_t)arr->region.size(d + 1);

        int64_t inner_size = arr->region.size(N - 1);

        std::array<int, N> idx = {};
        while (true) {
            int64_t global_offset = 0;
            int64_t local_offset = 0;
            for (int d = 0; d < N; ++d) {
                global_offset += ((int64_t)global_region.start[d] + idx[d]) * global_strides[d];
                local_offset += (int64_t)idx[d] * local_strides[d];
            }
            // Send as bytes: offset and size in bytes
            int64_t byte_offset = global_offset * esz;
            int64_t byte_size = inner_size * esz;
            char* base = static_cast<char*>(arr->data_ptr());
            dag_proxy[0].gather(epoch, name, byte_offset, byte_size, base + local_offset * esz);

            int d = N - 2;
            while (d >= 0) {
                if (++idx[d] < arr->region.size(d))
                    break;
                idx[d] = 0;
                --d;
            }
            if (d < 0)
                break;
        }
    }
}

// Explicit template instantiations for PartitionImpl
template class PartitionImpl<1>;
template class PartitionImpl<2>;
template class PartitionImpl<3>;
