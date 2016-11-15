FROM awswift/amazonlinux:0.1

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

RUN pip install --upgrade --no-cache-dir https://get.graphlab.com/GraphLab-Create/2.1/osman@berkeley.edu/8AF4-0201-9286-9731-83EA-4F5E-4DC7-94DC/GraphLab-Create-License.tar.gz

