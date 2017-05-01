if [[ "v1.2.3-SNAPSHOT" =~ ^v[0-9]+\.[0-9]+(\.[0-9]+)?(-M[0-9]+)?$ ]]; then
  echo "** Matched v1.2.3-SNAPSHOT **"
else 
  echo "Didn't match v1.2.3-SNAPSHOT"; fi

if [[ "v1.2.3" =~ ^v[0-9]+\.[0-9]+(\.[0-9]+)?(-M[0-9]+)?$ ]]; then
  echo "** Matched v1.2.3 **"
else 
  echo "Didn't match v1.2.3"; fi


if [[ "1.2.3" =~ ^v[0-9]+\.[0-9]+(\.[0-9]+)?(-M[0-9]+)?$ ]]; then
  echo "** Matched 1.2.3 **"
else 
  echo "Didn't match 1.2.3"; fi


if [[ "v1.2.3-M1" =~ ^v[0-9]+\.[0-9]+(\.[0-9]+)?(-M[0-9]+)?$ ]]; then
  echo "** Matched v1.2.3-M1 **"
else 
  echo "Didn't match v1.2.3-M1"; fi

if [[ "v1.2.3-M1-SNAPSHOT" =~ ^v[0-9]+\.[0-9]+(\.[0-9]+)?(-M[0-9]+)?$ ]]; then
  echo "** Matched v1.2.3-M1-SNAPSHOT **"
else 
  echo "Didn't match v1.2.3-M1-SNAPSHOT"; fi

if [[ "v1.2.3-X1" =~ ^v[0-9]+\.[0-9]+(\.[0-9]+)?(-M[0-9]+)?$ ]]; then
  echo "** Matched v1.2.3-X1 **"
else 
  echo "Didn't match v1.2.3-X1"; fi



