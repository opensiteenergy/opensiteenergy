#!/bin/bash

# Script to setup SSL certificate and restart services once build has completed

while true
    do
        sleep 1

        if [ -f "/usr/src/opensiteenergy/DOMAIN" ]; then
            . /usr/src/opensiteenergy/DOMAIN
            sudo sed -i "s/server_name _;/server_name $DOMAIN;/" /etc/nginx/sites-available/001-opensiteenergy-live.conf
            sudo /usr/sbin/nginx -s reload
            sudo rm /usr/src/opensiteenergy/log-certbot.txt
            sudo certbot --nginx --non-interactive --agree-tos --redirect --keep-until-expiring --email info@${DOMAIN} --domains ${DOMAIN} | sudo tee /usr/src/opensiteenergy/log-certbot.txt >/dev/null
            if grep -q 'Successfully deployed certificate' /usr/src/opensiteenergy/log-certbot.txt; then
                sudo cp /usr/src/opensiteenergy/DOMAIN /usr/src/opensiteenergy/DOMAINACTIVE
                sudo /usr/bin/systemctl restart tileserver.service
            fi
            sudo rm /usr/src/opensiteenergy/DOMAIN
        fi

        if [ -f "/usr/src/opensiteenergy/RESTARTSERVICES" ]; then
            echo "Restarting tileserver.service and apache2 with post-build conf"
            sudo /usr/bin/systemctl restart tileserver.service
            sudo ln -s /etc/nginx/sites-available/001-opensiteenergy-live.conf /etc/nginx/sites-enabled/
            sudo rm -f /etc/nginx/sites-enabled/002-default-build-pre.conf
            sudo /usr/sbin/nginx -s reload
            rm /usr/src/opensiteenergy/RESTARTSERVICES
        fi

    done
