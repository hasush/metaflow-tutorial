from metaflow import FlowSpec, step


class HelloWorldFlow(FlowSpec):
    @step
    def start(self):
        print("Hello World Flow is starting.")
        self.next(self.hello)

    @step
    def hello(self):
        print("Hello World!")
        self.next(self.end)

    @step
    def end(self):
        print("Hello World Flow is stopping.")


if __name__ == "__main__":
    HelloWorldFlow()
