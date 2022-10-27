#!/bin/sh

#####################################################################
#
# script name: env.sh
# created in: 21/19/22
# modified in: 09:14:41
#
# summary: gera um ambiente virtual python automatizado
#                                               developed by: bates
#####################################################################

#variables
clear


# testing if python already exist

if  [ $? -eq 0 ] > /dev/null
then
    echo "Checkingn if the programs are installed properly"
    if which python3 > /dev/null
    then
        echo "Python Installed"
        python3 --version
        if which pip3 > /dev/null
        then
            echo "Pip Installed"
            pip3 --version
        fi

    else
        echo "Python Not Installed"
        sleep 2
        echo "Installing the software"
        sudo apt update -y
        sudo apt upgrade -y
        sudo apt install python3 -y
        sudo apt install python3-pip -y
        sudo apt install python3-venv -y
        clear
        sleep 2
        echo "Python Installed"
        python3 --version
    fi

    echo "Creating a environment"
    sleep 2
    python3 -m venv python_env
    . python_env/bin/activate

   

    echo "Environment call $NAME_ENV was created and activate with sucess!"
else
    echo "You get a error!"
fi

