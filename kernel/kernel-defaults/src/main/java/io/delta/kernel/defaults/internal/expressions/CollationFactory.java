package io.delta.kernel.defaults.internal.expressions;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;
import io.delta.kernel.expressions.CollationIdentifier;
import io.delta.kernel.internal.util.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import static io.delta.kernel.defaults.internal.expressions.CollationFactory.Collation.DEFAULT_COLLATION;
import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.STRING_COMPARATOR;
import static io.delta.kernel.expressions.CollationIdentifier.*;

public class CollationFactory {
  private static final Map<CollationIdentifier, Collation> collationMap = new ConcurrentHashMap<>();

  public static Collation fetchCollation(String collationName) {
    if (collationName.startsWith("UTF8")) {
      return fetchCollation(new CollationIdentifier(PROVIDER_SPARK, collationName));
    } else {
      return fetchCollation(new CollationIdentifier(PROVIDER_ICU, collationName));
    }
  }

  public static Collation fetchCollation(CollationIdentifier collationIdentifier) {
    if (collationIdentifier.equals(DEFAULT_COLLATION_IDENTIFIER)) {
      return DEFAULT_COLLATION;
    } else if (collationMap.containsKey(collationIdentifier)) {
      return collationMap.get(collationIdentifier);
    } else {
      Collation collation;
      if (collationIdentifier.getProvider().equals(PROVIDER_SPARK)) {
        collation = UTF8CollationFactory.fetchCollation(collationIdentifier);
      } else if (collationIdentifier.getProvider().equals(PROVIDER_ICU)) {
        collation = ICUCollationFactory.fetchCollation(collationIdentifier);
      } else {
        throw new IllegalArgumentException(String.format("Invalid collation provider: %s.", collationIdentifier.getProvider()));
      }
      collationMap.put(collationIdentifier, collation);
      return collation;
    }
  }

  private static class UTF8CollationFactory {
    private static Collation fetchCollation(CollationIdentifier collationIdentifier) {
      if (collationIdentifier.equals(DEFAULT_COLLATION_IDENTIFIER)) {
        return DEFAULT_COLLATION;
      } else {
        // TODO UTF8_LCASE
        throw new IllegalArgumentException(String.format("Invalid collation identifier: %s.", collationIdentifier));
      }
    }
  }

  private static class ICUCollationFactory {
    /**
     * Bit 17 in collation ID having value 0 for case-sensitive and 1 for case-insensitive
     * collation.
     */
    private enum CaseSensitivity {
      CS, CI
    }

    /**
     * Bit 16 in collation ID having value 0 for accent-sensitive and 1 for accent-insensitive
     * collation.
     */
    private enum AccentSensitivity {
      AS, AI
    }

    /**
     * Mapping of locale names to corresponding `ULocale` instance.
     */
    private static final Map<String, ULocale> ICULocaleMap = new HashMap<>();

    private static Collation fetchCollation(CollationIdentifier collationIdentifier) {
      if (collationIdentifier.getVersion().isPresent() &&
              !collationIdentifier.getVersion().get().equals(ICU_COLLATOR_VERSION)) {
        throw new IllegalArgumentException(String.format("Invalid collation version: %s.", collationIdentifier.getVersion().get()));
      }

      String locale = getICULocale(collationIdentifier);

      Tuple2<CaseSensitivity, AccentSensitivity> caseAndAccentSensitivity = getICUCaseAndAccentSensitivity(collationIdentifier, locale);
      CaseSensitivity caseSensitivity = caseAndAccentSensitivity._1;
      AccentSensitivity accentSensitivity = caseAndAccentSensitivity._2;

      Collator collator = getICUCollator(locale, caseSensitivity, accentSensitivity);

      return new Collation(
              collationIdentifier,
              collator::compare);
    }

    private static String collationName(String locale, CaseSensitivity caseSensitivity, AccentSensitivity accentSensitivity) {
      StringBuilder builder = new StringBuilder();
      builder.append(locale);
      if (caseSensitivity != CaseSensitivity.CS) {
        builder.append('_');
        builder.append(caseSensitivity.toString());
      }
      if (accentSensitivity != AccentSensitivity.AS) {
        builder.append('_');
        builder.append(accentSensitivity.toString());
      }
      return builder.toString();
    }

    private static String getICULocale(CollationIdentifier collationIdentifier) {
      String collationName = collationIdentifier.getName();
      String collationNameUpperCase = collationIdentifier.getName().toUpperCase();

      // Search for the longest locale match because specifiers are designed to be different from
      // script tag and country code, meaning the only valid locale name match can be the longest
      // one.
      int lastPos = -1;
      for (int i = 1; i <= collationNameUpperCase.length(); i++) {
        String localeName = collationNameUpperCase.substring(0, i);
        if (ICULocaleMap.containsKey(localeName)) {
          lastPos = i;
        }
      }
      if (lastPos == -1) {
        throw new IllegalArgumentException(String.format("Invalid collation name: %s.", collationIdentifier.toStringWithoutVersion()));
      } else {
        return collationName.substring(0, lastPos);
      }
    }

    private static Tuple2<CaseSensitivity, AccentSensitivity> getICUCaseAndAccentSensitivity(CollationIdentifier collationIdentifier, String locale) {
      String collationName = collationIdentifier.getName();

      // Try all combinations of AS/AI and CS/CI.
      CaseSensitivity caseSensitivity;
      AccentSensitivity accentSensitivity;
      if (collationName.equals(locale) ||
              collationName.equals(locale + "_AS") ||
              collationName.equals(locale + "_CS") ||
              collationName.equals(locale + "_AS_CS") ||
              collationName.equals(locale + "_CS_AS")
      ) {
        caseSensitivity = CaseSensitivity.CS;
        accentSensitivity = AccentSensitivity.AS;
      } else if (collationName.equals(locale + "_CI") ||
              collationName.equals(locale + "_AS_CI") ||
              collationName.equals(locale + "_CI_AS")) {
        caseSensitivity = CaseSensitivity.CI;
        accentSensitivity = AccentSensitivity.AS;
      } else if (collationName.equals(locale + "_AI") ||
              collationName.equals(locale + "_CS_AI") ||
              collationName.equals(locale + "_AI_CS")) {
        caseSensitivity = CaseSensitivity.CS;
        accentSensitivity = AccentSensitivity.AI;
      } else if (collationName.equals(locale + "_AI_CI") ||
              collationName.equals(locale + "_CI_AI")) {
        caseSensitivity = CaseSensitivity.CI;
        accentSensitivity = AccentSensitivity.AI;
      } else {
        throw new IllegalArgumentException(String.format("Invalid collation name: %s.", collationIdentifier.toStringWithoutVersion()));
      }

      return new Tuple2<>(caseSensitivity, accentSensitivity);
    }

    private static Collator getICUCollator(String locale, CaseSensitivity caseSensitivity, AccentSensitivity accentSensitivity) {
      ULocale.Builder builder = new ULocale.Builder();
      builder.setLocale(ICULocaleMap.get(locale));
      // Compute unicode locale keyword for all combinations of case/accent sensitivity.
      if (caseSensitivity == CaseSensitivity.CS &&
              accentSensitivity == AccentSensitivity.AS) {
        builder.setUnicodeLocaleKeyword("ks", "level3");
      } else if (caseSensitivity == CaseSensitivity.CS &&
              accentSensitivity == AccentSensitivity.AI) {
        builder
                .setUnicodeLocaleKeyword("ks", "level1")
                .setUnicodeLocaleKeyword("kc", "true");
      } else if (caseSensitivity == CaseSensitivity.CI &&
              accentSensitivity == AccentSensitivity.AS) {
        builder.setUnicodeLocaleKeyword("ks", "level2");
      } else if (caseSensitivity == CaseSensitivity.CI &&
              accentSensitivity == AccentSensitivity.AI) {
        builder.setUnicodeLocaleKeyword("ks", "level1");
      }
      ULocale resultLocale = builder.build();
      Collator collator = Collator.getInstance(resultLocale);
      // Freeze ICU collator to ensure thread safety.
      collator.freeze();
      return collator;
    }

    static {
      ICULocaleMap.put("UNICODE", ULocale.ROOT);
      // ICU-implemented `ULocale`s which have corresponding `Collator` installed.
      ULocale[] locales = Collator.getAvailableULocales();
      // Build locale names in format: language["_" optional script]["_" optional country code].
      // Examples: en, en_USA, sr_Cyrl_SRB
      for (ULocale locale : locales) {
        // Skip variants.
        if (locale.getVariant().isEmpty()) {
          String language = locale.getLanguage();
          // Require non-empty language as first component of locale name.
          assert (!language.isEmpty());
          StringBuilder builder = new StringBuilder(language);
          // Script tag.
          String script = locale.getScript();
          if (!script.isEmpty()) {
            builder.append('_');
            builder.append(script);
          }
          // 3-letter country code.
          String country = locale.getISO3Country();
          if (!country.isEmpty()) {
            builder.append('_');
            builder.append(country);
          }
          String localeName = builder.toString();
          // Verify locale names are unique.
          assert (!ICULocaleMap.containsKey(localeName.toUpperCase()));
          ICULocaleMap.put(localeName.toUpperCase(), locale);
        }
      }
    }
  }

  public static class Collation {

    public static Collation DEFAULT_COLLATION = new Collation(DEFAULT_COLLATION_IDENTIFIER, STRING_COMPARATOR);

    public Collation(CollationIdentifier collationIdentifier, Comparator<String> collationComparator) {
      this.identifier = collationIdentifier;
      this.comparator = collationComparator;
      this.equalsFunction = (s1, s2) -> this.comparator.compare(s1, s2) == 0;
    }

    public CollationIdentifier getCollationIdentifier() {
      return identifier;
    }

    public Comparator<String> getComparator() {
      return comparator;
    }

    public BiFunction<String, String, Boolean> getEqualsFunction() {
      return equalsFunction;
    }

    private final CollationIdentifier identifier;
    private final Comparator<String> comparator;
    private final BiFunction<String, String, Boolean> equalsFunction;
  }
}
