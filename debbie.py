import argparse
import tarfile
from itertools import chain
from json import dumps, loads
from logging import INFO, basicConfig, getLogger
from pathlib import Path
from random import shuffle
from subprocess import DEVNULL, check_call
from typing import Dict, List
from urllib.request import urlretrieve
from uuid import uuid4

import requests
from bs4 import BeautifulSoup
from deb_pkg_tools.package import find_object_files
from tqdm import tqdm

from endpoints import ALL_PKGS, PKG_DEB, PKG_FTP_NAME, PKG_INFO, PKG_SRC, PKG_VERSION
from langs import LANG_MAPS, LANGS

basicConfig(format="%(asctime)s - %(message)s", level=INFO)
logger = getLogger(__name__)


def dl_deb(package: str, target: Path) -> str:
    dl_page = requests.get(PKG_DEB.format(package))
    s = BeautifulSoup(dl_page.text, "html.parser")
    hrefs = list(
        filter(
            lambda h: PKG_FTP_NAME in h,
            map(lambda l: l["href"], s.find_all("a", href=True)),
        )
    )

    if hrefs:
        urlretrieve(hrefs[0], filename=target / f"{package}.deb")
        return str((target / f"{package}.deb").resolve())
    else:
        return ""


def get_metadata(langs: List[str]) -> List[Dict[str, str]]:
    """
    Get metadata for all packages matching any of the provided languages.

    :param langs: List of permitted languages. Filtered by dominant language
        in the project.
    """
    try:
        acceptable = list(map(lambda l: LANG_MAPS[l], langs))
    except:
        logger.error(f"Invalid language in: {langs}")
        raise AssertionError

    pkgs = requests.get(ALL_PKGS).json()
    packages = []

    for package in tqdm(list(map(lambda p: p["name"], pkgs["packages"]))):
        try:
            version = requests.get(PKG_VERSION.format(package)).json()
            v = version["versions"][0]["version"]
            info = requests.get(PKG_INFO.format(package, v)).json()
            top_lang = info["pkg_infos"]["sloc"][0][0]
            if top_lang in acceptable:
                pkginfo = (package, top_lang, v, PKG_SRC.format(package))
                packages.append(pkginfo)
        except Exception as e:
            logger.info(f"Non-fatal error for package {package}. Skipping. {e}")
            continue
    return packages


def run(
    langs: List[str],
    cache: Path,
    target: Path,
    keywords: List[str],
    sample_size: int,
    debs: bool,
) -> None:

    if cache is None or not cache.exists():
        packages = get_metadata(langs)
        with open(cache, "w") as f:
            f.write(dumps(packages))
    else:
        with open(cache, "r") as f:
            packages = loads(f.read())

    try:
        target.mkdir(parents=True, exist_ok=False)
    except:
        pass

    result = []

    if sample_size != 0:
        shuffle(packages)

    for package, top_lang, version, dpage in tqdm([p.values() for p in packages]):
        try:
            if debs:
                pkgpath = target / package
                pkgpath.mkdir(parents=True, exist_ok=True)
                output = dl_deb(package, pkgpath)
                if output:
                    check_call(
                        ["dpkg-deb", "-xv", f"{output}", f"{pkgpath}"],
                        stdout=DEVNULL,
                        stderr=DEVNULL,
                    )
                    result.append(
                        (package, top_lang, version, dpage, find_object_files(pkgpath))
                    )
            else:
                dpcontent = requests.get(dpage)
                s = BeautifulSoup(dpcontent.text, "html.parser")
                hrefs = list(
                    filter(
                        lambda h: "orig.tar.gz" in h,
                        map(lambda l: l["href"], s.find_all("a", href=True)),
                    )
                )
                if len(hrefs) > 0:
                    href = hrefs[0]
                    pkgpath = target / package
                    pkgpath.mkdir(parents=True, exist_ok=True)
                    untar_target = str(pkgpath / (package + ".tar.gz"))

                    urlretrieve(href, filename=untar_target)

                    tar = tarfile.open(untar_target, "r:gz")
                    tar.extractall(path=pkgpath / package)
                    tar.close()

                    res = 0
                    ok_suffixes = list(
                        chain.from_iterable(map(lambda l: LANGS[l], langs))
                    )
                    for fname in pkgpath.rglob("*"):
                        if (
                            fname.is_file()
                            and "".join(map(lambda s: s.lower(), fname.suffixes))
                            in ok_suffixes
                        ):
                            with open(fname, "rb") as f:
                                content = f.read()
                                if any(k in content for k in keywords) or not keywords:
                                    res = 1

                    if (pkgpath / package).exists():
                        result.append((package, top_lang, version, dpage, res))

        except Exception as e:
            logger.info(f"Error processing package {package}: {e}")
            continue

        if len(result) > sample_size and sample_size != 0:
            break

    with open(target / "res.json", "w") as f:
        f.write(dumps(result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A small tool to scrape the Debian APT repository for sources."
    )
    parser.add_argument(
        "--languages",
        "-l",
        nargs="+",
        required=True,
        help=f"A list of languages to filter for. Options: {','.join(LANG_MAPS.keys())}",
    )
    parser.add_argument(
        "--sample-size",
        "-s",
        action="store",
        type=int,
        required=False,
        default=0,
        help="Randomly samples n packages from the filtered list. Default: uses full list.",
    )
    parser.add_argument(
        "--cache",
        "-c",
        nargs="?",
        default=None,
        type=Path,
        const=Path(__file__).with_name("cache.json"),
        required=False,
        help="A cache file path, or whether to use cache.json.",
    )
    parser.add_argument(
        "--target",
        "-t",
        action="store",
        type=Path,
        default=uuid4(),
        required=False,
        help="Target directory to download sources to.",
    )
    parser.add_argument(
        "--keywords",
        "-k",
        nargs="+",
        default=[],
        required=False,
        help="A list of keywords to search for in the source files.",
    )
    parser.add_argument(
        "--debs",
        "-d",
        action="store_true",
        required=False,
        default=False,
        help="Whether to download .debs instead of .tar.gz files.",
    )
    args = parser.parse_args()
    run(
        langs=args.languages,
        cache=args.cache,
        target=args.target,
        keywords=args.keywords,
        sample_size=args.sample_size,
        debs=args.debs,
    )
