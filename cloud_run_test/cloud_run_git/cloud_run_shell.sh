

count_nm='14'

docker build -t test_image${count_nm}:tag${count_nm} .
docker tag test_image${count_nm}:tag${count_nm}  asia-northeast3-docker.pkg.dev/owenchoi-404302/cloud-run-test/test_image${count_nm}:tag${count_nm}
docker images
docker push  asia-northeast3-docker.pkg.dev/owenchoi-404302/cloud-run-test/test_image${count_nm}:tag${count_nm}
