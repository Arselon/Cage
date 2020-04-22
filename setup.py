import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cage_api",
    version="2.6",
    description="Cage system API for client's remote file access",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Arselon/Cage",
    author="Arslan Aliev",
    author_email="arslanaliev@yahoo.com",
    maintainer="Arslan Aliev",
    maintainer_email="arslanaliev@yahoo.com",	
    license="Apache",
    packages=setuptools.find_packages(), 	
    keywords= ['Cage', 'remote file access'],
	python_requires=">=3.7",	
    classifiers=[
        "Programming Language :: Python :: 3.7",
	    "Intended Audience :: Developers",	
    ],
)