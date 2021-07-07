import os
from pathlib import Path
from typing import *
import subprocess

def maybe_stringify_path(path: Path, stringify: bool) -> Union[str, Path]:
    if stringify: return str(path)
    else: return path

class AWSCredential:
  aws_access_key: str
  aws_secret_key: str
  aws_username: str 

  _verbose: bool = False

  def execute_command(self, cmd_string: str):
    if self._verbose: print(cmd_string)
    os.system(cmd_string)

  def authenticate(self, aws_mfa_code = None):
    self.execute_command("mkdir -p ~/.aws")
    self.execute_command("\n".join(["cat <<EOF > ~/.aws/credentials",
              "[default-long-term]",
              f"aws_access_key_id = {self.aws_access_key}",
              f"aws_secret_access_key = {self.aws_secret_key}",
              "EOF"]))
    if aws_mfa_code is not None: 
      self.execute_command(f"echo {aws_mfa_code} | aws-mfa --device arn:aws:iam::034489990011:mfa/{self.aws_username}")

def maybe_stringify_path(path: Path, stringify: bool) -> Union[str, Path]:
    if stringify: return str(path)
    else: return path

class VirufyDataset:

    dataset_names = (
        'virufy-cdf-india-clinical-1',
        'virufy-cdf-coughvid', 
        'virufy-cdf-coswara',
        'virufy-cdf-iatos',
        'virufy-cdf-bolivia',
        'virufy-cdf-peru',
        'virufy-cdf-argentina',
        'virufy-cdf-brazil',
        'virufy-cdf-colombia',
        'virufy-cdf-mexico',
    )

    def __init__(self, dataset_name: str, root_dir: str = 'datasets'):
        assert dataset_name in self.dataset_names, f"Dataset not recognized. Choices: {self.dataset_names}"
        self.dataset_name = dataset_name        
        self.root_dir = Path(root_dir)
        self.get_dataset_dir(as_string=False).mkdir(exist_ok=True, parents=True)

    """ Client-facing code. We guarantee an unchanging interface to these functions """

    def get_dataset_dir(self, as_string = True) -> Union[str, Path]:
        return maybe_stringify_path(self.root_dir / self.dataset_name, as_string)

    def get_raw_label_filepath(self, as_string = True) -> Union[str, Path]: 
        """ Get the filepath for the raw label """
        label_fp = self.get_dataset_dir(as_string = False) / f"{self.dataset_name}-label.csv"
        return maybe_stringify_path(label_fp, as_string)

    def get_raw_audio_dir(self, as_string = True) -> Union[str, Path]: 
        """ Get the directory containing all audio files """ 
        audio_dir = self.get_dataset_dir(as_string = False) / f'{self.dataset_name}-audio'
        return maybe_stringify_path(audio_dir, as_string)

    def iter_audio_filepaths(self):
        for audio_fp in self.get_raw_audio_dir(as_string = False).iterdir():
            yield audio_fp

    def iter_audio_keys(self):
        """ yield audio files names without suffix """
        for audio_fp in self.get_raw_audio_dir(as_string = False).iterdir():
            yield audio_fp.stem

    def is_downloaded(self) -> bool:
        """ Check if the dataset is downloaded """
        audio_dir = self.get_raw_audio_dir(as_string=False)
        label_fp = self.get_raw_label_filepath(as_string=False)
        return audio_dir.exists() and any(audio_dir.iterdir()) and label_fp.exists()
    
    def download(self):
        """ Download dataset to self.root_dir
        
        Audio will be in self.get_raw_audio_dir() 
        Label will be in self.get_raw_label_filepath()
        """
        if self.dataset_name not in self.dataset_names:
            raise ValueError(f'Dataset {self.dataset_name} not recognized. Choices: {self.dataset_names}')
        
        if self.is_downloaded():
            self.log(f"Dataset {self.dataset_name} already downloaded.")
            return
        dataset_dir = self.get_dataset_dir(as_string = False)
        self.log(f"Downloading {self.dataset_name} to {dataset_dir}")
        self._download_virufy_data(dataset_dir, self.dataset_name)

    """ Backend-facing code. These functions may change """

    def log(self, message: str):
        print(message)

    @staticmethod 
    def _download_virufy_data(dataset_dir, dataset_name) -> None:
        audio_dirpath = str(dataset_dir / f'{dataset_name}-audio')
        label_filepath = str(dataset_dir / f'{dataset_name}-label.csv')

        # def os_system_with_exceptions(cmd):
        #     lst = subprocess.run(cmd)
        #     if 0 != lst.returncode
        #         raise ValueError(f'Command failed: {cm}')

        # Remove previously cached data
        os.system(f"rm -rf {str(dataset_dir / f'{dataset_name}-audio')}")
        os.system(f"rm -rf {str(dataset_dir / f'{dataset_name}-label.csv')}")
        os.system(f"rm -rf {dataset_name}")

        # Download the audio files
        # os.system(f"aws s3 cp s3://{dataset_name}/{dataset_name}.zip {dataset_name}.zip")
        subprocess.run(['wget', f"https://{dataset_name}.s3.us-east-2.amazonaws.com/{dataset_name}.zip"], check=True)
        os.system(f"unzip {dataset_name}-val.zip")
        os.system(f"mv {dataset_name} {audio_dirpath}" )
        os.system(f"rm -rf {dataset_name}.zip")

        # Download the csv
        subprocess.run(['wget', f"https://{dataset_name}.s3.us-east-2.amazonaws.com/{dataset_name}.csv"], check=True)
        os.system(f"mv {dataset_dir / f'{dataset_name}.csv'} {label_filepath}")
