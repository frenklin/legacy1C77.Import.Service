using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Security.Cryptography;

namespace Legacy1C77.Import.Service.DAL
{
    public static class StringUtils
    {


        public static Guid StringToGUID(this string value)
        {
            MD5 md5Hasher = MD5.Create();
            byte[] data = md5Hasher.ComputeHash(Encoding.Default.GetBytes(value));
            return new Guid(data);
        }

        public static string Encode(this string value, Encoding src, Encoding trg)
        {
            Decoder dec = src.GetDecoder();
            byte[] ba = trg.GetBytes(value);
            int len = dec.GetCharCount(ba, 0, ba.Length);
            char[] ca = new char[len];
            dec.GetChars(ba, 0, ba.Length, ca, 0);
            return new string(ca);
        }
    }
}
