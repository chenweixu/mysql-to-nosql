#!/bin/bash

work_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../" && pwd )"
cd $work_dir

cp $work_dir/conf/devel.yaml $work_dir/conf.yaml

mkdir $work_dir/log