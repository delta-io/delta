/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Dynamically injected post-processing code for the API docs */

$(document).ready(function() {
  addBadges(":: Unstable ::", '<span class="unstable badge">Unstable API</span>');
  addBadges(":: DeveloperApi ::", '<span class="developer badge">Developer API</span>');
  addBadges(":: Evolving ::", '<span class="evolving badge">Evolving API</span>');
});

function addBadges(tag, html) {
  var tags = $(".block:contains(" + tag + ")")

  // Remove identifier tags
  tags.each(function(index) {
    var oldHTML = $(this).html();
    var newHTML = oldHTML.replace(tag, "");
    $(this).html(newHTML);
  });

  // Add html badge tags
  tags.each(function(index) {
    if ($(this).parent().is('td.colLast')) {
      $(this).parent().prepend(html);
    } else if ($(this).parent('li.blockList')
                      .parent('ul.blockList')
                      .parent('div.description')
                      .parent().is('div.contentContainer')) {
      var contentContainer = $(this).parent('li.blockList')
                                    .parent('ul.blockList')
                                    .parent('div.description')
                                    .parent('div.contentContainer')
      var header = contentContainer.prev('div.header');
      if (header.length > 0) {
        header.prepend(html);
      } else {
        contentContainer.prepend(html);
      }
    } else if ($(this).parent().is('li.blockList')) {
      $(this).parent().prepend(html);
    } else {
      $(this).prepend(html);
    }
  });
}
