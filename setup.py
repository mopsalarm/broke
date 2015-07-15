#!/usr/bin/env python3

from distutils.core import setup

setup(name="broke",
      version="1.0",
      description="A very simple event broker/queue with flatfiles",
      author="Mopsalarm",
      author_email="mopsalarm@users.noreply.github.com",
      url="https://github.com/mopsalarm/broke",
      packages=["broke"],
      install_requires=[
          "construct==2.5.2",
          "portalocker==0.5.4",
          "six==1.9.0"
      ])
