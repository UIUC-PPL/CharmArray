### C++ server
### Building
This is the code for the c++ server that uses LibcharmTyles to service the python frontend. This needs all of libcharmtyles dependencies to be setup and correct paths to be added to [config.cmake](config.cmake). 
1. Setting up Kokkos and Kokkos-kernels : Use the setupGPU/setup  script in LibcharmTyles
2. Setting up charm : we used the non-smp netlrts cuda build for this. Build from charm++'s `hapi_portable` branch, using the command
```
./build charm++ netlrts-linux-x86_64  cuda --with-production --force -j16
```
3. Setting up Eigen - refer eigen documentation(CPU only build)

then the server can be build with GPU support using
```
cmake .. -DCharm_ENABLE_GPU=ON
```
```
Note: the cpu buid needs some changes(ifdefing a little amount of code inside the compilation module)
```
### Running
Currently it's only been tested with the non-smp build. It's passes correctness tests for one node tests.
command:
```
./charmrun   +p 8 ./server.out   ++server  ++server-port 10000  ++local -r 10000   -c 10000  -v  10000 +gpuipceventpool 32 +allgpus
```
 This needs the mps server running for multiplexing multiple contexts on the same GPU.
```
nvidia-cuda-mps-control -d
```

details about the options-
++server/++server/+gpuipceventpool/+gpucommbuffer port are charm++ hapi/ccs options


-r : row size for matrix chunks

-c : column size for matrix chunks

-v : size of vector chunks

### Known pitfalls
- calling `get` on large matrices leads to an internal charm++ error most probably in the custom reductions of charm++ because of integer overflows.