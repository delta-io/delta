require=(function e(t,n,r){function s(o,u){if(!n[o]){if(!t[o]){var a=typeof require=="function"&&require;if(!u&&a)return a(o,!0);if(i)return i(o,!0);var f=new Error("Cannot find module '"+o+"'");throw f.code="MODULE_NOT_FOUND",f}var l=n[o]={exports:{}};t[o][0].call(l.exports,function(e){var n=t[o][1][e];return s(n?n:e)},l,l.exports,e,t,n,r)}return n[o].exports}var i=typeof require=="function"&&require;for(var o=0;o<r.length;o++)s(r[o]);return s})({"sphinx-rtd-theme":[function(require,module,exports){ // NOLINT
    var jQuery = (typeof(window) != 'undefined') ? window.jQuery : require('jquery');

    // Sphinx theme nav state
    function ThemeNav () {

        var nav = {
            navBar: null,
            win: null,
            winScroll: false,
            winResize: false,
            linkScroll: false,
            winPosition: 0,
            winHeight: null,
            docHeight: null,
            isRunning: null
        };

        nav.enable = function () {
            var self = this;

            jQuery(function ($) {
                self.init($);

                self.reset();
                self.win.on('hashchange', self.reset);

                // Set scroll monitor
                self.win.on('scroll', function () {
                    if (!self.linkScroll) {
                        self.winScroll = true;
                    }
                });
                setInterval(function () { if (self.winScroll) self.onScroll(); }, 25);

                // Set resize monitor
                self.win.on('resize', function () {
                    self.winResize = true;
                });
                setInterval(function () { if (self.winResize) self.onResize(); }, 25);
                self.onResize();
            });
        };

        nav.init = function ($) {
            var doc = $(document),
                self = this;

            this.navBar = $('div.wy-side-scroll:first');
            this.win = $(window);

            // Set up javascript UX bits
            $(document)
            // Shift nav in mobile when clicking the menu.
                .on('click', "[data-toggle='wy-nav-top']", function() {
                    $("[data-toggle='wy-nav-shift']").toggleClass("shift");
                    $("[data-toggle='rst-versions']").toggleClass("shift");
                })

            // Nav menu link click operations
                .on('click', ".wy-menu-vertical .current ul li a", function() {
                    var target = $(this);
                    // Close menu when you click a link.
                    $("[data-toggle='wy-nav-shift']").removeClass("shift");
                    $("[data-toggle='rst-versions']").toggleClass("shift");
                    // Handle dynamic display of l3 and l4 nav lists
                    self.toggleCurrent(target);
                    self.hashChange();
                })
                .on('click', "[data-toggle='rst-current-version']", function() {
                    $("[data-toggle='rst-versions']").toggleClass("shift-up");
                })

            // Make tables responsive
            $("table.docutils:not(.field-list)")
                .wrap("<div class='wy-table-responsive'></div>");

            // Add expand links to all parents of nested ul
            $('.wy-menu-vertical ul').not('.simple').siblings('a').each(function () {
                var link = $(this);
                expand = $('<span class="toctree-expand"></span>');
                expand.on('click', function (ev) {
                    self.toggleCurrent(link);
                    ev.stopPropagation();
                    return false;
                });
                link.prepend(expand);
            });

            setupSearch();
        };

        nav.reset = function () {
            // Get anchor from URL and open up nested nav
            var anchor = encodeURI(window.location.hash);
            if (anchor) {
                try {
                    var link = $('.wy-menu-vertical')
                        .find('[href="' + anchor + '"]');
                    $('.wy-menu-vertical li.toctree-l1 li.current')
                        .removeClass('current');
                    link.closest('li.toctree-l2').addClass('current');
                    link.closest('li.toctree-l3').addClass('current');
                    link.closest('li.toctree-l4').addClass('current');
                }
                catch (err) {
                    console.log("Error expanding nav for anchor", err);
                }
            }
        };

        nav.onScroll = function () {
            this.winScroll = false;
            var newWinPosition = this.win.scrollTop(),
                winBottom = newWinPosition + this.winHeight,
                navPosition = this.navBar.scrollTop(),
                newNavPosition = navPosition + (newWinPosition - this.winPosition);
            if (newWinPosition < 0 || winBottom > this.docHeight) {
                return;
            }
            this.navBar.scrollTop(newNavPosition);
            this.winPosition = newWinPosition;
        };

        nav.onResize = function () {
            this.winResize = false;
            this.winHeight = this.win.height();
            this.docHeight = $(document).height();
        };

        nav.hashChange = function () {
            this.linkScroll = true;
            this.win.one('hashchange', function () {
                this.linkScroll = false;
            });
        };

        nav.toggleCurrent = function (elem) {
            var parent_li = elem.closest('li');
            parent_li.siblings('li.current').removeClass('current');
            parent_li.siblings().find('li.current').removeClass('current');
            parent_li.find('> ul li.current').removeClass('current');
            parent_li.toggleClass('current');
        }

        return nav;
    };

    module.exports.ThemeNav = ThemeNav();

    if (typeof(window) != 'undefined') {
        window.SphinxRtdTheme = { StickyNav: module.exports.ThemeNav };
    }

},{"jquery":"jquery"}]},{},["sphinx-rtd-theme"]);

// CUSTOM JS

// managing embedded notebooks
$(".embedded-notebook").each(function() {
  var $container = $(this);
  var $iframe = $(this).find("iframe");

  // Currently, notebooks over a certain size are not embedded.
  // These should not be augmented with Javascript.
  if ($container.hasClass('too-large')) {
    return;
  }

  var $expandCTA = $('<button class="expand-cta">Loading notebook...</button>')

  $expandCTA.on('click', () => {
    if ($container.hasClass('expanded')) {
      $container.removeClass('expanded')
      $expandCTA.text('Expand notebook ▼')
      // Move scroll to the top of the container on collapse;
      // adjust up 200px to account for the fixed header.
      $('body, html').scrollTop($container.offset().top - 200);
    } else {
      $container.addClass('expanded')
      $expandCTA.text('Collapse notebook ▲')
    }
  });

  $container.append($expandCTA)

  $iframe.on("load", function() {
    var iframe = this;

    // Remove loading state
    $container.addClass("loaded");
    $expandCTA.text('Expand notebook ▼')

    // The iframe's contents can change as assets are loading in. Give it a check periodically,
    // and update the container height if there's any difference.
    function updateIframeHeight() {
      var loadableFrame = iframe.contentWindow.document.getElementsByClassName("overallContainer")[0];
      var currentHeight = loadableFrame.scrollHeight;
      iframe.height = (currentHeight + 10) + "px";
    }

    setTimeout(updateIframeHeight, 0);
    setInterval(updateIframeHeight, 5000);
  });

  // Load it
  $(this).waypoint({
    offset: "100%",
    handler: function() {
      this.destroy(); // turn off the Waypoint
      $iframe.attr("src", $iframe.data("src"));
    }
  });
});

// clipboard stuff
$('.highlight').each(function() {
    var btn = '<button class="clippy" aria-label="Copy sample to clipboard">' +
        '<img src="_static/clippy.svg" alt="Copy to clipboard">Copy</button>';
    $(this).prepend(btn);
});

var clippy = new Clipboard('.clippy', {
    target: function(trigger) {
        return trigger.nextSibling;
    }
});

clippy.on("success", function(e) {
    var $clippy = $(e.trigger);
    $clippy.addClass("copied").find("span").text("Copied!");
    e.clearSelection();
    setTimeout(function(){
        $clippy.removeClass("copied").find("span").text("Copy");
    }, 1000);
});

function maybeScrollToAnchor() {
    scrollTo($(location.hash));
    return true;
}

function scrollTo($scrollingAnchor) {
    if ($scrollingAnchor.length) {
        var elScrollTop = $scrollingAnchor.hasClass('section')
            ? $scrollingAnchor.find(':header').position().top
            : $scrollingAnchor.position().top;

        $('html, body').animate({
            scrollTop: elScrollTop
        }, 10);
    }
}

$(document).ready(function () {
    function makeNavItem(heading) {
        var $a = $(heading).find('a');
        var $link = $('<a></a>').attr('href', $a.last().attr('href')).text(heading.textContent.trim());
        return $('<li class="affix-container__item"></li>')
            .append($link);
    }

    //build nav menu
    var $headings = $('.js-page-container h2');
    if ($headings.length > 1) {
        //desktop nav
        var $nav = $('<nav class="affix-container affix-container--desktop js-affix"><div class="affix-container--ltr">' +
            '<h3 class="affix-container__desktop-title">In this article:</h3><ul></ul></div></nav>');
        var $ul = $nav.find('ul');

        $headings.each(function () {
            var $li = makeNavItem(this);
            $ul.append($li);
            this.navLink = $li;//keep the appropriate na item
        });

        $('.js-page-container').append($nav);

        //mobile nav
        var $section = $('h1').nextAll('.section').first();
        if ($section.length) {
            var $nav_m = $('<div class="affix-container--mobile"><strong class="affix-container__title">In this article:</strong><ul></ul></div>');
            var $ul_m = $nav_m.find('ul');
            $headings.each(function () {
                var $li = makeNavItem(this);
                $ul_m.append($li);
            });
            $nav_m.insertBefore($section);
        }
    }

    //nav menu selection
    if ($headings.length > 1) {
        function findDisplayedH2Item() {
            var headerHeight = 220;
            for (var i = $headings.length - 1; i >= 0; i--) {
                if ($headings[i].getBoundingClientRect().top <= headerHeight) {
                    return $headings[i];
                }
            }
            return $headings[0];
        }

        var animationFrame = 0;
        function scheduleUpdate() {
            cancelAnimationFrame(animationFrame);
            animationFrame = requestAnimationFrame(updateH2Selection);
        }

        function updateH2Selection() {
            const selectedHeading = findDisplayedH2Item();
            selectH2ItemInSideOutline(selectedHeading);
        }

        function selectH2ItemInSideOutline(heading) {
            if (heading === null) {
                return;
            }

            const current = $('.js-affix .selected');
            if (current[0] !== heading.navLink[0]) {
                current.removeClass('selected');
                heading.navLink.addClass('selected');
            }
        }

        window.addEventListener('scroll', () => {
			scheduleUpdate();
		}, { passive: true });

		// listen for manual content updates
		window.addEventListener('content-update', () => {
			scheduleUpdate();
		});

		scheduleUpdate();
    }
});

$(document).ready(function () {
    var languageNames = {
      'text': '',
      'default':'',
      'sql':'SQL',
      'dotnet':'.NET',
      'xml':'XML',
      'json':'JSON',
      'http':'HTTP',
      'md':'Markdown',
      'yaml':'YAML',
      'ini':'ini',
      'spacy':'spaCy'
    };
    function formatLanguageName(lang) {
        return languageNames.hasOwnProperty(lang) ? languageNames[lang] : lang.replace(/^\w/, s => s.toUpperCase());
    }
    var reLanguageParam = /language-\w+/;
    function getSelectedLanguage() {
        var params = window.location.hash.substr(1).split('&');
        for (var i = 0; i < params.length; i++) {
            var m = params[i].match(reLanguageParam);
            if (m) {
                return m[0];
            }
        }
        return null;
    }
    function setSelectedLanguage(selected) {
        var params = window.location.hash.substr(1).split('&').filter(function(v){return !!v.length});
        var replaced = false;
        for (var i = 0; i < params.length; i++) {
            var m = params[i].match(reLanguageParam);
            if (m) {
                replaced = true;
                params[i] = selected;
            }
        }
        if (!replaced) {
            params.push(selected);
        }
        var hash = '#' + params.join('&');
        window.history.replaceState(window.history.state, '' , hash);
    }
    function getLanguageId(language) {
        return 'language-' + language;
    }
    function buildHeader(elem, index) {
        var lang = elem.hasAttribute('lang')
            ? elem.getAttribute('lang')
            : elem.classList[1].substr(10);
        var title = formatLanguageName(lang);
        var langId = getLanguageId(lang);
        var targetId = index + '-' + langId;
        var ownId = index + '-anchor-' + langId;
        var header = $('<li class="clt-tabs-item" role="tab" data-lang="' + langId + '" data-tab-id="' + targetId + '" data-id="' + ownId + '">' +
            '<a class="clt-tabs-item__href" aria-controls="' + targetId + '" href="#' + targetId + '" id="' + ownId + '">' + title + '</a></li>');
        header.find('.clt-tabs-item__href').click(onTabClick);
        return header;
    }
    function onTabClick(e) {
        var selected = $(this).parent().attr('data-lang');
        updateTabsSelection(selected);
        setSelectedLanguage(selected);
        e.preventDefault();
        e.stopImmediatePropagation();
    }
    function onKeyDown(e) {
        var update = false;
        var selected = this.querySelector('.clt-tabs-item--selected');
        var index = Array.prototype.indexOf.call(this.childNodes, selected);
        switch (e.keyCode) {
            case 37: // left/up
            case 38: {
                if (index === 0) {
                    index = this.childNodes.length - 1;
                } else {
                    index--;
                }
                update = true;
                break;
            }
            case 39: { // right
                if (index >= this.childNodes.length - 1) {
                    index = 0;
                } else {
                    index++;
                }
                update = true;
                break;
            }
            case 36: { // home
                index = 0;
                update = true;
                break;
            }
            case 35: { // home
                index = this.childElementCount - 1;
                update = true;
                break;
            }
        }
        if (update) {
            var lang = this.childNodes[index].getAttribute('data-lang');
            updateTabsSelection(lang);
            setSelectedLanguage(lang);
            $(this.childNodes[index]).find('.clt-tabs-item__href').focus();
            e.preventDefault();
            e.stopImmediatePropagation();
        }
    }
    function buildTabs(tab, index) {
        var literal = Array.prototype.indexOf.call(tab.classList, 'js-code-language-tabs--literal') > 0
            ? ' clt-tabs--literal'
            : '';
        var $tabs = $('<div class="clt-tabs' + literal +'"><ul class="clt-tabs__items" role="tablist" aria-label="Code block">' +
            '</ul><div class="clt-tabs__content"></div></div>');
        var $headers = $tabs.find('.clt-tabs__items');
        var $content = $tabs.find('.clt-tabs__content');

        while (tab.children.length) {
            var child = tab.children[0];
            var $li = buildHeader(child, index);
            child.classList.add('clt-tabs-content');
            child.setAttribute('id', $li.attr('data-tab-id'));
            child.setAttribute('data-lang', $li.attr('data-lang'));
            child.setAttribute('aria-controlled-by', $li.attr('data-id'));
            child.setAttribute('role', 'tabpanel');
            $headers.append($li);
            $content.append(child);
        }

        $headers.keydown(onKeyDown);
        tab.parentElement.replaceChild($tabs[0], tab);
    }
    function markSelected($header, $content) {
        $header.addClass('clt-tabs-item--selected')
                        .find('.clt-tabs-item__href')
                        .attr({'aria-selected': true, 'tabindex': 0});
        $content.addClass('clt-tabs-content--selected');
        $content.attr({'aria-hidden': false});
    }
    function markUnselected($headers, $contents) {
        $headers.removeClass('clt-tabs-item--selected')
                .find('.clt-tabs-item__href')
                .attr({'aria-selected': false, 'tabindex': -1});
        $contents.removeClass('clt-tabs-content--selected');
        $contents.attr({'aria-hidden': true});
    }
    function updateTabSelection(tab, selected){
        var $headers = $(tab).find('.clt-tabs-item');
        var $contents = $(tab).find('.clt-tabs-content');
        markUnselected($headers, $contents);
        if (selected) {
            var $header = $headers.filter('[data-lang="' + selected + '"]');
            var $content = $contents.filter('[data-lang="' + selected + '"]');
            if ($header.length) {
                markSelected($header, $content);
            } else {
                markSelected($headers.first(), $contents.first());
            }
        } else {
            markSelected($headers.first(), $contents.first());
        }
    }
    function updateTabsSelection(selected) {
        selected = selected || getSelectedLanguage();
        $('.clt-tabs').each(function () {
            updateTabSelection(this, selected);
        });
    }
    function getCodeBlockLangFromClass(codeBlock) {
        var reLang = /highlight-([^\s]+)/
        var m = codeBlock.className.match(reLang);
        if (m) {
            return formatLanguageName(m[1]);
        } else {
            return '';
        }
    }
    function enhanceCodeBlocks() {
        $('.highlight').filter(function() {
            return !$(this).closest('.js-code-language-tabs--literal').length;
        }).each(function() {
            var lang = $(this).closest('.js-code-language-tabs').length ? '' : getCodeBlockLangFromClass(this.parentElement);
            $(this).prepend('<div class="highlight-header"><span class="highlight-header__lang">' + lang + '</span></div>');
        });
    }
    enhanceCodeBlocks();
    $('.js-code-language-tabs').each(function (index) {
        buildTabs(this, index);
    });
    updateTabsSelection();
});

$(document).ready(function () {
    var keyboard = true;
    var clsNoOutline = 'no-outline';
    window.addEventListener('keydown', function(e) {
        if (e.code === 'Tab' && !e.ctrlKey && !e.altKey) {
            if (!keyboard) {
                keyboard = true;
                document.body.classList.remove(clsNoOutline);
            }
        }
    });
    window.addEventListener('mousedown', function(e) {
        if (keyboard) {
            keyboard = false;
            document.body.classList.add(clsNoOutline);
        }
    });
});

$(document).ready(function () {
    $('.js-tutorial-steps a').attr('target', '_blank');
});

function setupSearch() {
    docsearch({
      apiKey: algoliaConfigs.key,
      indexName: algoliaConfigs.index,
      inputSelector: '#algolia-search',
      debug: false,
      autocompleteOptions: {
        appendTo: '#algolia-wrapper',
        hint: false
      }
    });
    new Tether({
        element: '#algolia-wrapper',
        target: '#algolia-search',
        attachment: 'bottom left',
        targetAttachment: 'top left',
        targetOffset: '20px 0',
        constraints: [{
            to: 'window',
            attachment: 'together'
        }]
    });
}
