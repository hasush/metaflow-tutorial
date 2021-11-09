import numpy as np
from metaflow import Flow, FlowSpec, Parameter, get_metadata, step
from PIL import Image


class ReadImageFlow2(FlowSpec):

    image_file_path = Parameter(
        "image_file_path",
        default="/workspaces/ml/data/image-tagger-v4-assets/data/image-tagger-v4/training_data/fireplace/fireplace_test_dataset_1_mls_images/361.jpg",
    )

    @step
    def start(self):
        run = Flow("ReadImageFlow").latest_successful_run
        self.image_file_path_1 = run.data.image_file_path_1
        self.z = 1
        self.next(self.uno)

    @step
    def uno(self):
        self.y = 2
        self.next(self.dos)

    @step
    def dos(self):
        self.x = 3
        print(self.image_file_path_1)
        self.next(self.begin)

    @step
    def begin(self):
        self.w = 4
        self.next(self.normalize_0_1, self.normalize_0_255)

    @step
    def normalize_0_1(self):
        image = Image.open(self.image_file_path)
        image = (image - np.min(image)) / (np.max(image) - np.min(image))
        self.image_0_1 = image
        self.next(self.join)

    @step
    def normalize_0_255(self):
        image = Image.open(self.image_file_path)
        image = (image - np.min(image)) / (np.max(image) - np.min(image)) * 255
        self.image_0_255 = image
        self.next(self.join)

    @step
    def join(self, inputs):
        self.image_0_1_join = inputs.normalize_0_1.image_0_1
        self.image_0_255_join = inputs.normalize_0_255.image_0_255
        self.a = 1
        self.next(self.almost)

    @step
    def almost(self):
        print(self.image_0_1_join)
        print(self.image_0_255_join)
        self.b = 2
        self.next(self.there)

    @step
    def there(self):
        self.c = 3
        self.next(self.end)

    @step
    def end(self):
        self.d = 4
        print(self.image_0_1_join)


if __name__ == "__main__":
    ReadImageFlow2()
