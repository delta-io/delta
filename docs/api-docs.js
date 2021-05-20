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
  var annotations = $("dt:contains('Annotations')").next("dd").children("span.name");
  addBadges(annotations, "Unstable", ":: Unstable ::", '<span class="unstable badge">Unstable API</span>');
  addBadges(annotations, "Developer", ":: DeveloperApi ::", '<span class="developer badge">Developer API</span>');
  addBadges(annotations, "Evolving", ":: Evolving ::", '<span class="evolving badge">Evolving API</span>');
});

function addBadges(allAnnotations, name, tag, html) {
  var annotations = allAnnotations.filter(":contains('" + name + "')")
  var tags = $(".cmt:contains(" + tag + ")")

  // Remove identifier tags from comments
  tags.each(function(index) {
    var oldHTML = $(this).html();
    var newHTML = oldHTML.replace(tag, "");
    $(this).html(newHTML);
  });

  // Add badges to all containers
  tags
    // Scala 2.11 docs require these
    .prevAll("h4.signature")
    .add(annotations.closest("div.fullcommenttop"))
    .add(annotations.closest("div.fullcomment").prevAll("h4.signature"))
    // Scala 2.12 docs require this
    .add(tags.prevAll("span.symbol"))
    .add(annotations.closest("div.fullcomment").prevAll("span.symbol"))
    .prepend(html);
}
