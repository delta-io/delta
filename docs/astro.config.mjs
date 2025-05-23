// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  site: "https://delta-docs-incubator.netlify.app/",
  image: {
    service: {
      entrypoint: "astro/assets/services/sharp",
    },
  },
  integrations: [
    starlight({
      customCss: ["./src/styles/custom.css"],
      title: "Delta Lake",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/delta-io/delta",
        },
      ],
      editLink: {
        baseUrl:
          "https://github.com/jakebellacera/db-site-staging/tree/main/sites/delta-docs",
      },
      lastUpdated: true,
      favicon: "/favicon.svg",
      logo: {
        light: "./src/assets/delta-lake-logo-light.svg",
        dark: "./src/assets/delta-lake-logo-dark.svg",
        replacesTitle: true,
      },
      sidebar: [
        { label: "Introduction", link: "/" },
        {
          label: "Apache Spark connector",
          collapsed: true,
          items: [
            {
              slug: "quick-start",
            },
            {
              slug: "delta-batch",
            },
            {
              slug: "delta-streaming",
            },
            {
              slug: "delta-update",
            },
            {
              slug: "delta-change-data-feed",
            },
            {
              slug: "delta-utility",
            },
            {
              slug: "delta-constraints",
            },
            {
              slug: "versioning",
            },
            {
              slug: "delta-default-columns",
            },
            {
              slug: "delta-column-mapping",
            },
            {
              slug: "delta-clustering",
            },
            {
              slug: "delta-deletion-vectors",
            },
            {
              slug: "delta-drop-feature",
            },
            {
              slug: "delta-row-tracking",
            },
            {
              slug: "delta-storage",
            },
            {
              slug: "delta-type-widening",
            },
            {
              slug: "delta-uniform",
            },
            {
              slug: "delta-sharing",
            },
            {
              slug: "concurrency-control",
            },
            {
              slug: "porting",
            },
            {
              slug: "best-practices",
            },
            {
              slug: "delta-faq",
            },
            {
              slug: "optimizations-oss",
            },
          ],
        },
        {
          slug: "delta-trino-integration",
        },
        {
          slug: "delta-presto-integration",
        },
        {
          slug: "redshift-spectrum-integration",
        },
        {
          slug: "snowflake-integration",
        },
        {
          slug: "bigquery-integration",
        },
        {
          slug: "flink-integration",
        },
        {
          slug: "delta-more-connectors",
        },
        {
          slug: "delta-kernel",
        },
        {
          slug: "delta-standalone",
        },
        {
          slug: "delta-apidoc",
        },
        {
          slug: "releases",
        },
        {
          slug: "delta-resources",
        },
        {
          slug: "table-properties",
        },
        {
          label: "Contribute",
          link: "https://github.com/delta-io/delta/blob/master/CONTRIBUTING.md",
          attrs: { target: "_blank" },
        },
      ],
    }),
  ],
});
