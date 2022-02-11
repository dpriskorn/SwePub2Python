from models.iso import IsoThreeLetterLanguageCode


class SwepubLanguage(IsoThreeLetterLanguageCode):
    """This models Swepubs implementation of ISO 639-3 in case it deviates"""

    def __str__(self):
        return f"{self.label}"
