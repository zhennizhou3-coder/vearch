cd ./build
sh build.sh
export LD_LIBRARY_PATH=/home/zhouzn/rebuild/vearch/build/gamma_build:$LD_LIBRARY_PATH
cd ..
./build/bin/vearch -conf ./config/config.toml all