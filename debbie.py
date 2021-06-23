import requests
import tarfile
import sys
from bs4 import BeautifulSoup
from tqdm import tqdm
from pprint import pformat
from random import shuffle 
from pathlib import Path
from uuid import uuid4
from urllib.request import urlretrieve


ALL_PKGS_ENDPOINT = "https://sources.debian.org/api/list"
PKG_VERSION_ENDPOINT = "https://sources.debian.org/api/src/{}/"
PKG_INFO_ENDPOINT = "https://sources.debian.org/api/info/package/{}/{}/"
SRC_ENDPOINT = "https://packages.debian.org/source/jessie/{}"
ACCEPTABLE_LANGS = ["ansic", "cpp"]
SAMPLE_SIZE = int(sys.argv[1])

print("Running with sample size ", SAMPLE_SIZE)

KEYWORDS = [
            b"decode",
            b"decrypt",
            b"decompress",
            b"inflate",
            b"dec",
            b"dec_str",
            b"huff"
        ]
C_EXT = [
            ".c",
            ".cc",
            ".cxx",
            ".cpp",
            ".h",
            ".hh",
            ".hxx",
            ".hpp"
        ]

if not Path("all.list").exists():
    pkgs = requests.get(ALL_PKGS_ENDPOINT).json()
    packages = []
    for package in tqdm(list(map(lambda p: p["name"], pkgs["packages"]))):
        try:
            version = requests.get(PKG_VERSION_ENDPOINT.format(package)).json()
            v = version["versions"][0]["version"]
            info = requests.get(PKG_INFO_ENDPOINT.format(package, v)).json()
            top_lang = info["pkg_infos"]["sloc"][0][0]
            if top_lang in ACCEPTABLE_LANGS:
                pkginfo = (package, top_lang, v, SRC_ENDPOINT.format(package))
                packages.append(pkginfo)
        except Exception as e:
            continue

    with open("all.list", "w") as f:
        f.write(pformat(packages))
else:
    with open("all.list") as f:
        packages = eval(f.read())

uuid = str(uuid4())
print("Working on UUID ", uuid)
dest = Path(uuid)
try:
    dest.mkdir(parents=True, exist_ok=False)
except:
    pass

result = []
shuffle(packages)
for package, top_lang, version, dpage in tqdm(packages):
    try:
        dpcontent = requests.get(dpage)
        s = BeautifulSoup(dpcontent.text, "html.parser")
        hrefs = list(filter(lambda h: 'orig.tar.gz' in h, map(lambda l: l["href"], s.find_all("a", href=True))))
        if len(hrefs) > 0:
            href = hrefs[0]
            pkgpath = dest / package
            pkgpath.mkdir(parents=True, exist_ok=True)
            target = str(pkgpath / (package + ".tar.gz"))
            urlretrieve(href, filename=target)
            tar = tarfile.open(target, "r:gz")
            tar.extractall(path=pkgpath / package)
            tar.close()
            res = 0
            for fname in pkgpath.rglob("*"):
                if fname.is_file() and "".join(map(lambda s: s.lower(), fname.suffixes)) in C_EXT:
                    with open(fname, "rb") as f:
                        content = f.read()
                        if any(k in content for k in KEYWORDS):
                            res = 1
            if (pkgpath / package).exists():
                result.append((package, top_lang, version, dpage, res))

    except:
        continue

    if len(result) > SAMPLE_SIZE:
        break

with open(dest / "res.json", "w") as f:
    f.write(pformat(result))
