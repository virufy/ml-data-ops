import pandas as pd
from random import randint
import unittest
from virufy_dataset import VirufyDataset

class TestVirufyDataset(unittest.TestCase):

    def test_public_download(self):
        dataset_name = 'virufy-cdf-india-clinical-1'
        dataset = VirufyDataset(dataset_name)
        dataset.download()

    # def test_private_download(self):
    #     dataset_name = 'virufy-cdf-peru'
    #     # VirufyDataset.aws_auth(mfa_code='1234')
    #     dataset = VirufyDataset(dataset_name)
    #     dataset.download()

    # def test_isupper(self):
    #     self.assertTrue('FOO'.isupper())
    #     self.assertFalse('Foo'.isupper())

    # def test_split(self):
    #     s = 'hello world'
    #     self.assertEqual(s.split(), ['hello', 'world'])
    #     # check that s.split fails when the separator is not a string
    #     with self.assertRaises(TypeError):
    #         s.split(2)

# Daniel
# aws_cred = AWSCredential(
#   aws_access_key = 'AKIAQQB6DPN5UYLMHJOZ',
#   aws_secret_key = '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',
#   aws_username = 'minami.yamaura@virufy.org'
# )
# aws_cred.authenticate('122105')

if __name__ == '__main__':
    unittest.main()