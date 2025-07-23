from setuptools import setup, find_packages

setup(
    name="wrc_rallydj",
    version="0.1.0",
    package_dir={"": "src/shinyapp"},
    packages=["wrc_rallydj"], #"otherpackage": "../../somewhere_else/src",
    install_requires=["numpy", "pandas", "sqlite_utils", "jupyterlite_simple_cors_proxy"],
    author="Tony Hirst",
    author_email="tony.hirst@gmail.com",
    description="RallyDatajunkie package for making requests to WRC live timing, results and data APIs.",
    long_description=open("PACKAGE_README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/RallyDataJunkie/wrc-shinylive",

    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
