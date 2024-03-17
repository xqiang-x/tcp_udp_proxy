# tcp_udp_proxy ， 反向代理
## 依赖libuv
###编译
<br>make dir build</br>
<br>cd build </br>
<br>cmake ..</br>
<br>make</br>

###服务端运行
<br> ./udp_proxy_server -i 0.0.0.0 -p 7002 -I 0.0.0.0 -P 8891 -d # -i udp监听地址，-p udp监听端口 -I tcp监听地址 -P tcp监听端口 -d 后台运行</br>

###客户端运行
<br>udp_proxy -m <tcp服务器地址> -n <tcp服务器端口> -M <要转发到的tcp地址> -N <要转发到的tcp端口> -i <udp服务器地址> -p <要转发的udp地址> -I <要转发的udp地址> -P <要转发的udp端口> -d -c 10(tcp预留连接数) </br>
<br>例如:</br>
<br> ./udp_proxy -m x.x.x.x -n 8891 -M 127.0.0.1 -N 80 -i x.x.x.x -p 7002 -I 127.0.0.1 -P 7011 -d -c 10 </br>

### 使用
<br> 连接到服务器的 8891 端口后，数据会被转发到 udpproxy的 -M 地址 -N 端口 </br>



