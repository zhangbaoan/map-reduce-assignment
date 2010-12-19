package pagerank;

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UnitTestDriver {

	public static void main(String[] args) {
//		String line = \"<page>     <title>Auld Inglis leid</title>          <text>The '''Auld Inglis''' leid is a wast [[Germanic leid]] that wis spak in [[Breetain]] atween aboot [[425]] an [[1125]].   Hit is an early furm o [[Inglis leid|Inglis]] an [[Scots leid|Scots]], an is sib wi [[Auld Frisian]] an [[Auld Saxon]].  Wast Saxon wis the heidmaist mak o Auld Inglis in the auncient corpus, includin the [[epic poem]] [[Beowulf]], as the Wast Saxons wis the strangest kinrick o thon time, while the Northumbrian dialect o Auld Inglis eventually becam the Scots leid.  O aw the descendants o Auld Inglis, Scots and Northumbrian Inglis are the maist true tae the oreeginal furm.   {| !Auld Inglis | !Scots |- |Eald Englisc | |Auld Inglis |- |hit | |hit |- |Ä“acod | |eikit |- |sibb | |sib |- |mÃ¦st | |maist |- |Èefunden | |fund |- |Ã¾oht | |thocht | |- |Åhsta | |oxter |}   {{stub}} [[Category:Leid]]  [[am:áŒ¥áŠ•á‰³á‹Š áŠ¥áŠ•áŒáˆŠá‹áŠ›]] [[ang:Englisc sprÇ£c]] [[ar:Ø¥Ù†Ø¬Ù„ÙŠØ²ÙŠØ© Ø¹ØªÙŠÙ‚Ø©]] [[ast:InglÃ©s antiguu]] [[be-x-old:Ð¡Ñ‚Ð°Ñ€Ð°Ð°Ð½Ð³ÐµÐ»ÑŒÑÐºÐ°Ñ Ð¼Ð¾Ð²Ð°]] [[bg:Ð¡Ñ‚Ð°Ñ€Ð¾Ð°Ð½Ð³Ð»Ð¸Ð¹ÑÐºÐ¸ ÐµÐ·Ð¸Ðº]] [[bn:à¦ªà§à¦°à¦¾à¦šà§€à¦¨ à¦‡à¦‚à¦°à§‡à¦œà¦¿]] [[ca:AnglÃ¨s antic]] [[cs:StarÃ¡ angliÄtina]] [[da:Angelsaksisk]] [[de:Altenglische Sprache]] [[el:Î‘ÏÏ‡Î±Î¯Î± Î±Î³Î³Î»Î¹ÎºÎ® Î³Î»ÏŽÏƒÏƒÎ±]] [[en:Old English]] [[eo:Anglosaksa lingvo]] [[es:Idioma anglosajÃ³n]] [[et:Vanainglise keel]] [[fa:Ø§Ù†Ú¯Ù„ÛŒØ³ÛŒ Ù‚Ø¯ÛŒÙ…]] [[fi:Muinaisenglanti]] [[fr:Anglo-saxon (langue)]] [[fy:Aldingelsk]] [[gl:InglÃ©s antigo]] [[glk:Ù‚Ø¯ÛŒÙ…ÛŒ Ø§ÛŒÙ†Ú¯ÛŒÙ„ÛŒØ³ÛŒ]] [[he:×× ×’×œ×™×ª ×¢×ª×™×§×”]] [[hi:à¤à¤‚à¤—à¥à¤²à¥‹-à¤¸à¥ˆà¤•à¥à¤¸à¥‰à¤¨ à¤­à¤¾à¤·à¤¾]] [[hu:Ã“angol nyelv]] [[id:Bahasa Inggris Kuno]] [[is:Fornenska]] [[it:Antico inglese]] [[ja:å¤è‹±èªž]] [[ka:áƒ«áƒ•áƒ”áƒšáƒ˜ áƒ˜áƒœáƒ’áƒšáƒ˜áƒ¡áƒ£áƒ áƒ˜ áƒ”áƒœáƒ]] [[ko:ê³ ëŒ€ ì˜ì–´]] [[la:Lingua Anglica antiqua]] [[lt:Senoji anglÅ³ kalba]] [[mk:Ð¡Ñ‚Ð°Ñ€Ð¾Ð°Ð½Ð³Ð»Ð¸ÑÐºÐ¸ Ñ˜Ð°Ð·Ð¸Ðº]] [[nah:HuehcÄuh InglatlahtÅlli]] [[nds:Angelsassische Sprake]] [[nl:Oudengels]] [[no:Gammelengelsk]] [[oc:Anglosaxon]] [[os:Ð Ð°Ð³Ð¾Ð½ Ð°Ð½Ð³Ð»Ð¸ÑÐ°Ð³ Ã¦Ð²Ð·Ð°Ð³]] [[pl:JÄ™zyk staroangielski]] [[pt:LÃ­ngua inglesa antiga]] [[ro:Limba englezÄƒ veche]] [[ru:Ð”Ñ€ÐµÐ²Ð½ÐµÐ°Ð½Ð³Ð»Ð¸Ð¹ÑÐºÐ¸Ð¹ ÑÐ·Ñ‹Ðº]] [[simple:Old English language]] [[sl:Stara angleÅ¡Äina]] [[sv:Fornengelska]] [[sw:Kiingereza cha Kale]] [[th:à¸ à¸²à¸©à¸²à¸­à¸±à¸‡à¸à¸¤à¸©à¹‚à¸šà¸£à¸²à¸“]] [[tr:Eski Ä°ngilizce]] [[vls:Oudiengels]] [[zh:å¤è‹±è¯­]]</text>   </page>\";
		String line = "8732711	\"Pussy Cats\" Starring The Walkmen	2009-12-14 19:39:33	<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<articles loadtime=\"0 sec\" rendertime=\"0.049 sec\" totaltime=\"0.049 sec\"><article><paragraph><template name=\"Infobox Album\">\n<param name=\"Name\">\"Pussy Cats\" Starring the Walkmen</param>\n<param name=\"Type\">studio</param>\n<param name=\"Artist\"><link><target>The Walkmen</target></link></param>\n<param name=\"Cover\">WalkmenPussyCats.jpg</param>\n<param name=\"Released\">October 24, 2006</param>\n<param name=\"Recorded\">2006</param>\n<param name=\"Genre\"><link><target>Indie rock</target></link></param>\n<param name=\"Length\">39:39</param>\n<param name=\"Label\"><link><target>Record Collection</target></link></param>\n<param name=\"Producer\"/>\n<param name=\"Reviews\"><list type=\"bullet\"><listitem><sentence><link><target>Allmusic</target></link>";
//		Pattern pattern = Pattern.compile(\"\\[\\[(.*?)\\]\\]\");
		Pattern pattern = Pattern.compile("\\<target>(.*?)\\</target>");
		Matcher m = pattern.matcher(line);
//		while (m.find()) {
//		    String s = m.group(1);
//		    if (s.indexOf("|") != -1)
//		    	s = s.substring(0, s.indexOf("|"));
//		    System.out.println(s);
//		}
//		
//		int beginIndex = line.indexOf("\t");
//		int endIndex = line.indexOf("<?");
//		String substr = line.substring(beginIndex, endIndex).trim();
//		substr = substr.substring(0, substr.indexOf("\t"));
//		System.out.println(substr.trim());
		
		String value = "0.15000000000000002, NOW (magazine), PopMatters, Rolling Stone, Stylus Magazine, Tiny Mix Tapes, Winnipeg Sun, Allmusic, A Hundred Miles Off, You And Me (The Walkmen album), The A.V. Club, Austin Chronicle, Pussy Cats, The Walkmen, 2006, 2006 in music, Harry Nilsson, Pussy Cats, John Lennon, Pussy Cats, A Hundred Miles Off, The Walkmen, Many Rivers to Cross, Jimmy Cliff, Subterranean Homesick Blues, Bob Dylan, Harry Nilsson, Harry Nilsson, Harry Nilsson, Save the Last Dance for Me, Doc Pomus, Mort Shuman, John Lennon, Harry Nilsson, Ted Vann, Harry Nilsson, Rock Around the Clock, Record Collection, James E. Myers, Max C. Freedman, Record Collection, Category:The Walkmen albums, Category:2006 albums, Entertainment Weekly, Pitchfork Media, Indie rock, ";
		String[] strs = value.split(",");
		for (int i = 0; i < strs.length; i++) {
			System.out.println(strs[i]);
		}
	}
}
