import subprocess
import os
from glob import glob
from pathlib import Path


class AWSGlueLocalDev:
    pull_image_cmd = "docker pull amazon/aws-glue-libs:glue_libs_2.0.0_image_01"
    map_host_port = "-p 4040:4040"
    map_notebook_port = "-p 8888:8888"  # The default port of Jupyter is 8888; Zeppelin is 8080.
    user_profile = os.environ['USERPROFILE']
    aws_credentials = Path(user_profile, ".aws")
    mount_credentials_dir = f"-v {aws_credentials}:/root/.aws:rw"
    local_notebook_dir = Path(user_profile, "Documents/notebooks")  # TODO - should be project directory
    powershell = list(Path(os.environ["SYSTEMROOT"], "System32", "WindowsPowerShell").glob("**/powershell.exe"))[0]
    print(powershell)

    mount_notebooks_dir = f"-v {local_notebook_dir}:/home/jupyter/jupyter_default_dir"
    aws_glue_image_v1 = "amazon/aws-glue-libs:glue_libs_1.0.0_image_01"
    aws_glue_image_v2 = "amazon/aws-glue-libs:glue_libs_2.0.0_image_01"
    start_notebook_server = "/home/jupyter/jupyter_start.sh"

    def __init__(self):
        self.__container_name = "glue_jupyter"
        self.get_remote_file("https://s3-us-west-2.amazonaws.com/crawler-public/json/serde/json-serde.jar")
        self.create_notebook_dir()
        # subprocess.run(
        #     f"{self.pull_docker_image()} "
        #     f"& {self.remove_container_if_exists()} "
        #     f"& {self.run_container()} "
        #     f"& {self.install_extra_packages()}",
        #     shell=True
        # )
        os.system(f"{self.install_extra_packages()}")
        # os.system(f"{self.pull_docker_image()} "
        #           f"& {self.remove_container_if_exists()} "
        #           f"& {self.run_container()} "
        #           f"& {self.install_extra_packages()}"
        #           )

    @classmethod
    def get_remote_file(cls, url):
        import requests
        data = requests.get(url)
        local_dir = Path(cls.local_notebook_dir, Path(url).name)
        if not local_dir.exists():
            print(f"downloading \"{url}\"")
            with open(Path(cls.local_notebook_dir, Path(url).name), "wb") as file:
                file.write(data.content)
                print(f"successfully downloaded \"{url}\" to \"{str(local_dir)}\"")

    def set_container_name(self):
        return f"--name {self.__container_name}"

    def create_notebook_dir(self):
        if not self.local_notebook_dir.exists():
            self.local_notebook_dir.mkdir(exist_ok=True)

    def pull_docker_image(self):
        return f"docker pull {self.aws_glue_image_v2}"

    def remove_container_if_exists(self):
        return f"docker rm -f /{self.__container_name}"

    def run_container(self):
        return f"docker run -itd " \
               f"{self.map_host_port} " \
               f"{self.map_notebook_port} " \
               f"{self.mount_credentials_dir} " \
               f"{self.mount_notebooks_dir} " \
               f"{self.set_container_name()} " \
               f" {self.aws_glue_image_v2} " \
               f"{self.start_notebook_server} "

    def install_extra_packages(self):
        packages = "deep-translator pandas==1.4.2"
        start_container = f"docker container start {self.__container_name}"
        return f"{start_container} " \
               f"& docker exec -it {self.__container_name} /nin/bash " \
               f"& python -m pip install --no-cache-dir {packages}"


if __name__ == "__main__":
    AWSGlueLocalDev()
