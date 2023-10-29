from scrapers.conama import ConamaScraper
from scrapers.legislacao_federal import LegislacaoFederalScraper
from scrapers.icmbio import ICMBIOScraper
from utils.files_remover import FilesRemover
from utils.files_uploader import FilesUploader


if __name__ == "__main__":
    # run scraper for CONAMA
    files_remover = FilesRemover()
    files_remover.start()
    files_uploader = FilesUploader(files_remover)

    # conama = ConamaScraper()
    # conama.run()

    # run scraper for Legislação Federal
    legislacao_federal = LegislacaoFederalScraper(
        files_remover=files_remover, files_uploader=files_uploader
    )
    filters = ["decretos"]
    legislacao_federal.run(filters)

    # run scraper for ICMBIO
    # icmbio = ICMBIOScraper(files_remover=files_remover, files_uploader=files_uploader)
    # icmbio.run()

    # wait for threads to finish
    files_remover.stop = True
    files_remover.join()
