#! /bin/bash -l

IS_MULTIPASS="false"

if [ -f /var/lib/cloud/instance/datasource ] && grep -q "DataSourceNoCloud" /var/lib/cloud/instance/datasource 2>/dev/null; then
    IS_MULTIPASS="true"
fi

if [ "$IS_MULTIPASS" = "true" ]; then
    echo "Multipass (NoCloud) detected"
    export EXTERNAL_IP=$(ip route get 1 | grep -oP 'src \K\S+')
else
    echo "Standard environment detected"
    export EXTERNAL_IP=$(curl ipinfo.io/ip)
fi

export LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=/opt/lib/pkgconfig:$PKG_CONFIG_PATH
export PUBLIC_URL=http://${EXTERNAL_IP}/tiles/

if [ -f "/usr/src/opensiteenergy/DOMAINACTIVE" ]; then
    . /usr/src/opensiteenergy/DOMAINACTIVE
    export PUBLIC_URL=https://${DOMAIN}/tiles/
fi

cd build/tileserver-live/
xvfb-run --auto-servernum --server-args="-screen 0 1024x768x24 +extension GLX +render -noreset" tileserver-gl -p 8080 --public_url ${PUBLIC_URL} --config config.json
