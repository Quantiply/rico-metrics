# -*- coding: utf-8 -*-
import re
import sys
import os.path


if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
    
    app_home = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
    
    sys.path.append(os.path.join(app_home, "app"))
    sys.path.append(os.path.join(app_home, "lib", "vendor"))

    import nose
    sys.exit(nose.run_exit())