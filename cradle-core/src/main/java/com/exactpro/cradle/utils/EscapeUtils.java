/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.utils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class EscapeUtils
{
	public static final char ESCAPE = '\\',
			DELIMITER = ':';
	public static final String ESCAPE_STR = Character.toString(ESCAPE),
			DELIMITER_STR = Character.toString(DELIMITER),
			ESCAPED_ESCAPE_STR = ESCAPE_STR+ESCAPE_STR,
			ESCAPED_DELIMITER_STR = ESCAPE_STR+DELIMITER;
	
	public static String escape(String s)
	{
		return s.replace(ESCAPE_STR, ESCAPED_ESCAPE_STR).replace(DELIMITER_STR, ESCAPED_DELIMITER_STR);
	}
	
	public static List<String> split(String s) throws ParseException
	{
		List<String> result = new ArrayList<>();
		
		StringBuilder sb = new StringBuilder();
		boolean escaped = false;
		for (int i = 0; i < s.length(); i++)
		{
			char c = s.charAt(i);
			if (escaped)
			{
				if (c == ESCAPE || c == DELIMITER)
					sb.append(c);
				else
					throw new ParseException("Invalid escape sequence at char "+i, i);
				
				escaped = false;
			}
			else
			{
				switch (c)
				{
				case ESCAPE : escaped = true; break;
				case DELIMITER  : 
					result.add(sb.toString());
					sb = new StringBuilder();
					break;
				default : sb.append(c); break;
				}
			}
		}
		
		if (escaped)
			throw new ParseException("Unused escape at string end", s.length()-1);
		
		result.add(sb.toString());
		return result;
	}
	
	public static String join(List<String> parts)
	{
		StringBuilder sb = new StringBuilder();
		for (String p : parts)
		{
			if (sb.length() > 0)
				sb.append(DELIMITER);
			sb.append(EscapeUtils.escape(p));
		}
		return sb.toString();
	}
}
