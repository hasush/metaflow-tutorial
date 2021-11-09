from io import StringIO

import numpy as np
from metaflow import FlowSpec, IncludeFile, step
from PIL import Image


class ReadImageFlow(FlowSpec):

    image_file_path_1 = "/workspaces/ml/data/image-tagger-v4-assets/data/image-tagger-v4/training_data/fireplace/fireplace_test_dataset_1_mls_images/361.jpg"
    image_file_path_2 = "/workspaces/ml/data/image-tagger-v4-assets/data/image-tagger-v4/training_data/fireplace/fireplace_test_dataset_1_mls_images/366.jpg"

    @step
    def start(self):
        self.image_file_paths = [self.image_file_path_1, self.image_file_path_2]
        self.asdf_asdf_value = 10
        self.next(self.load_image, foreach="image_file_paths")

    @step
    def load_image(self):
        self.image_file_path = self.input
        image = Image.open(self.image_file_path)
        print(f"Type of the image is: {type(np.array(image))}")
        self.next(self.join)

    @step
    def join(self, inputs):
        for input in inputs:
            print(input.image_file_path)
        self.asdf_value = 5
        self.next(self.end)

    @step
    def end(self):
        pass


# class ReadImageFlow(FlowSpec):

#     image_file_path_1 = "/workspaces/ml/data/image-tagger-v4-assets/data/image-tagger-v4/training_data/fireplace/fireplace_test_dataset_1_mls_images/361.jpg"
#     image_file_path_2 = "/workspaces/ml/data/image-tagger-v4-assets/data/image-tagger-v4/training_data/fireplace/fireplace_test_dataset_1_mls_images/366.jpg"
#     image_file_path_3 = IncludeFile(
#         "image_file_path_3",
#         default="/workspaces/ml/data/image-tagger-v4-assets/data/image-tagger-v4/training_data/fireplace/fireplace_test_dataset_1_mls_images/371.jpg",
#     )

#     @step
#     def start(self):
#         self.image_file_paths = [self.image_file_path_1, self.image_file_path_2, StringIO(self.image_file_path_3)]

#         self.next(self.load_image, foreach="image_file_paths")

#     @step
#     def load_image(self):
#         self.image_file_path = self.input
#         image = Image.open(self.image_file_path)
#         print(f"Type of the image is: {type(np.array(image))}")
#         self.next(self.join)

#     @step
#     def join(self, inputs):
#         for input in inputs:
#             print(input.image_file_path)
#         self.next(self.end)

#     @step
#     def end(self):
#         pass


# class ReadImageFlow(FlowSpec):

#     image_file_path_1 = IncludeFile(
#         "image_file_path_1",
#         default="/workspaces/ml/data/image-tagger-v4-assets/data/image-tagger-v4/training_data/fireplace/fireplace_test_dataset_1_mls_images/361.jpg",
#     )
#     image_file_path_2 = IncludeFile(
#         "image_file_path_2",
#         default="/workspaces/ml/data/image-tagger-v4-assets/data/image-tagger-v4/training_data/fireplace/fireplace_test_dataset_1_mls_images/366.jpg",
#     )
#     image_file_path_3 = IncludeFile(
#         "image_file_path_3",
#         default="/workspaces/ml/data/image-tagger-v4-assets/data/image-tagger-v4/training_data/fireplace/fireplace_test_dataset_1_mls_images/371.jpg",
#     )

#     @step
#     def start(self):
#         print(type(StringIO(self.image_file_path_1)))
#         self.next(self.end)
#         self.image_file_paths = [self.image_file_path_1, self.image_file_path_2, self.image_file_path_3]
#         self.next(self.load_image, foreach="image_file_paths")

#     @step
#     def load_image(self):
#         self.image_file_path = self.input
#         image = Image.open(StringIO(self.image_file_path))
#         print(f"Type of the image is: {type(np.array(image))}")
#         self.next(self.join)

#     @step
#     def join(self, inputs):
#         for input in inputs:
#             print(input.image_file_path)
#         self.next(self.end)

#     @step
#     def end(self):
#         pass


if __name__ == "__main__":
    ReadImageFlow()
