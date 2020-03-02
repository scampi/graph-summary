/*******************************************************************************
 * Copyright (c) 2012 National University of Ireland, Galway. All Rights Reserved.
 *
 *
 * This project is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This project is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this project. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.sindice.core.analytics;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.sindice.core.analytics.stats.assembly.AnalyticsProperties;

/**
 * @author Diego Ceccarelli <diego.ceccarelli@deri.org>
 * @link www.di.unipi.it/~ceccarel
 */
public class TestAnalyticsProperties {

  @Test
  public void Test()
  throws IOException {
    AnalyticsProperties properties = new AnalyticsProperties();
    InputStream in = getClass().getResourceAsStream("/util/test.properties");
    properties.load(in);

    assertEquals(properties.getReplacement("www.creativecommons.org/ns/something"), "creativecommons.org/ns#");
    assertEquals(properties.getReplacement("www.livejournal.org/rss/lj/1.0/something"), null);
    assertEquals(properties.isRemove("www.livejournal.org/rss/lj/1.0/something"), true);
    assertEquals(properties.isRemove("www.georss.org/georss/"), true);
    assertEquals(properties.isRemove("www.di.unipi.it/~ceccarel/"), false);
    assertEquals(properties.getReplacement("purl.org/dc/terms#relation"), "purl.org/dc/terms/");
    assertEquals(properties.getReplacement("wwww.di.unipi.it/test/###~diego"), null);
    assertEquals(properties.getReplacement("wwww.di.unipi.it/test2/test2"), "wwww.di.unipi.it/test/#");
    assertEquals(properties.getReplacement("wwww.di.unipi.it/diego/test2"), "test.it/test2");
    assertEquals(properties.getReplacement("www.somethingofdifferent.eu"), null);
    assertEquals(properties.isNormalizationEnabled(), true);
    assertEquals(properties.isIncludeClasses(), true);
  }

}
