using ProtoBuf;
namespace Server.Data
{
    /// <summary>
    /// 新区冲级达人数据
    /// </summary>
    [ProtoContract]
    class NewZoneUpLevelData
    {

        /// <summary>
        /// 数据
        /// </summary>
        [ProtoMember(1)]
        public List<NewZoneUpLevelItemData>? Items;

    }
}
