#
# Copyright 2014-2015 Quantiply Corporation. All rights reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import sys
import logging

def bootstrap(app_home):
    #Add app and lib/vendor directories to the Python path
    sys.path.append(app_home + "/app")
    sys.path.append(app_home + "/lib/vendor")

def create_entrypoint(full_name):
    # The Java code expects the class to be a global var named com_quantiply_rico_entrypoint
    global com_quantiply_rico_entrypoint

    if full_name is None:
        raise ValueError("Python entrypoint is None. Make sure it is specified properly in the properties.")
    
    tmp = full_name.rsplit(".")
    if len(tmp) < 2:
        raise ValueError("Please specify full name of the class. Looks like the module name is missing.")
        
    module_name,class_name = full_name.rsplit(".", 1)
    try:
        com_quantiply_rico_entrypoint = str_to_class(module_name, class_name)
    except Exception, e:
        logging.error("Error while loading %s. The most likely cause is that you are importing something that doesnot exist or there is a typo in the imports.\n" % (full_name, ))
        raise e

# Credit where it is due : http://stackoverflow.com/a/24674853
def str_to_class(module_name, class_name):
    import importlib
    module_ = importlib.import_module(module_name)
    class_ = getattr(module_, class_name)()
    return class_ or None
